package k_anonymity

import (
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"math"
	"math/rand"
)

// 数据集，包含内置的dataframe与数据集的设置
type DataSet struct {
	dataFrame     dataframe.DataFrame
	dataSetConfig DataSetConfig
}

func NewDataSet(df dataframe.DataFrame, dfConfig DataSetConfig) (*DataSet, error) {
	//todo check
	dataSet := new(DataSet)
	var seriesList []series.Series
	for idx, qualityName := range df.Names() {
		if config, ok := dfConfig.order[qualityName]; ok {
			vals := make([]interface{}, df.Nrow(), df.Nrow())
			for k := 0; k < df.Nrow(); k++ {
				v := df.Elem(k, idx).String()
				orderQuality := OrderQuality{
					Min: v,
					Max: v,
				}
				str, _ := config.OrderQualityFuncStruct.Marshal(orderQuality)
				vals[k] = str
			}
			orderSeries := series.New(vals, series.String, qualityName)
			seriesList = append(seriesList, orderSeries)
		} else if config, ok := dfConfig.disorder[qualityName]; ok {
			vals := make([]interface{}, df.Nrow(), df.Nrow())
			for k := 0; k < df.Nrow(); k++ {
				v := df.Elem(k, idx).String()
				disorderQuality := DisorderQuality{
					Set: []string{v},
				}
				str, _ := config.DisorderQualityFuncStruct.Marshal(disorderQuality)
				vals[k] = str
			}
			disorderSeries := series.New(vals, series.String, qualityName)
			seriesList = append(seriesList, disorderSeries)
		} else {
			seriesList = append(seriesList, df.Col(qualityName))
		}
	}
	dataFrame := dataframe.New(seriesList...)
	dataSet.dataFrame = dataFrame
	dataSet.dataSetConfig = dfConfig
	return dataSet, nil
}

func (d DataSet) LClustering() (dataframe.DataFrame, error) {
	var Q []dataframe.DataFrame
	var rQ []dataframe.DataFrame
	var D, DSharp dataframe.DataFrame
	D = d.dataFrame.Copy()
	if !lCheck(D, d.dataSetConfig) {
		return DSharp, fmt.Errorf("无法产生符合l=%d的结果数据集", d.dataSetConfig.l)
	}
	for lCheck(D, d.dataSetConfig) {
		randIdx := rand.Intn(D.Nrow())
		t := D.Subset(randIdx)
		D = removeRow(D, randIdx)

		G := t
		rG, err := generalize(G.Copy(), d.dataSetConfig)
		if err != nil {
			return DSharp, err
		}
		sValMap := map[interface{}]struct{}{}
		var colIdx int
		for i, name := range G.Names() {
			if name == d.dataSetConfig.s {
				colIdx = i
			}
		}
		for i := 0; i < G.Nrow(); i++ {
			sValMap[G.Elem(i, colIdx)] = struct{}{}
		}
		for !lCheck(G, d.dataSetConfig) {
			idx, minDistanceVal := 0, math.MaxFloat64
			for i := 0; i < D.Nrow(); i++ {
				if _, ok := sValMap[D.Elem(i, colIdx)]; ok {
					continue
				}
				distanceVal, err := distance4(1, G.Nrow(), D.Subset(i), rG, d.dataSetConfig)
				if err != nil {
					return DSharp, err
				}
				if distanceVal < minDistanceVal {
					idx = i
					minDistanceVal = distanceVal
				}
			}
			t_ := D.Subset(idx)
			if len(Q) == 0 {
				D = removeRow(D, idx)
				G = G.RBind(t_)
				rG, err = generalize(rG.RBind(t_), d.dataSetConfig)
				if err != nil {
					return DSharp, err
				}
			} else {
				GIdx, GMinDistance := 0, math.MaxFloat64
				for i := range Q {
					distanceVal, err := distance4(Q[i].Nrow(), G.Nrow(), rQ[i], rG, d.dataSetConfig)
					if err != nil {
						return DSharp, err
					}
					if distanceVal < GMinDistance {
						GIdx = i
						GMinDistance = distanceVal
					}
				}
				distance, err := distance4(1, G.Nrow(), t_, rG, d.dataSetConfig)
				if err != nil {
					return DSharp, err
				}
				if distance <= GMinDistance {
					D = removeRow(D, idx)
					G = G.RBind(t_)
					rG, err = generalize(rG.RBind(t_), d.dataSetConfig)
					if err != nil {
						return DSharp, err
					}
				} else {
					G_ := Q[GIdx]
					Q = append(Q[:GIdx], Q[GIdx+1:]...)
					rQ = append(rQ[:GIdx], rQ[GIdx+1:]...)
					G = G.RBind(G_)
					rG_, err := generalize(G_, d.dataSetConfig)
					if err != nil {
						return DSharp, err
					}
					rG, err = generalize(rG.RBind(rG_), d.dataSetConfig)
					if err != nil {
						return DSharp, err
					}
				}
			}
		}
		Q = append(Q, G)
		rQ = append(rQ, rG)
	}
	for D.Nrow() > 0 {
		randIdx := rand.Intn(D.Nrow())
		t_ := D.Subset(randIdx)
		D = removeRow(D, randIdx)
		DIdx, DMinDistance := 0, math.MaxFloat64
		for i := range Q {
			distanceVal, err := distance4(Q[i].Nrow(), 1, rQ[i], t_, d.dataSetConfig)
			if err != nil {
				return DSharp, err
			}
			if distanceVal < DMinDistance {
				DIdx = i
				DMinDistance = distanceVal
			}
		}
		Q[DIdx] = Q[DIdx].RBind(t_)
		var err error
		rQ[DIdx], err = generalize(rQ[DIdx].RBind(t_), d.dataSetConfig)
		if err != nil {
			return DSharp, err
		}
	}
	names := d.dataFrame.Names()
	seriesList := make([]series.Series, 0, len(names))
	for i, name := range names {
		if config, ok := d.dataSetConfig.order[name]; ok {
			vals := make([]string, 0, d.dataFrame.Nrow())
			for j := range rQ {
				orderStr := rQ[j].Elem(0, i).String()
				orderQuality,err := config.OrderQualityFuncStruct.Unmarshal(orderStr)
				if err != nil {
					return DSharp, err
				}
				val, err := config.OrderQualityFuncStruct.FormatFunc(orderQuality)
				if err != nil {
					return DSharp, err
				}
				for k := 0; k < Q[j].Nrow(); k++ {
					vals = append(vals, val)
				}
			}
			seriesList = append(seriesList, series.New(vals, series.String, name))
		} else if config, ok := d.dataSetConfig.disorder[name]; ok {
			vals := make([]string, 0, d.dataFrame.Nrow())
			for j := range rQ {
				disorderStr := rQ[j].Elem(0, i).String()
				disorderQuality,err := config.DisorderQualityFuncStruct.Unmarshal(disorderStr)
				if err != nil {
					return DSharp, err
				}
				val, err := config.DisorderQualityFuncStruct.FormatFunc(disorderQuality)
				if err != nil {
					return DSharp, err
				}
				for k := 0; k < Q[j].Nrow(); k++ {
					vals = append(vals, val)
				}
			}
			seriesList = append(seriesList, series.New(vals, series.String, name))
		} else {
			var vals series.Series
			for j := range Q {
				if j == 0 {
					vals = Q[0].Col(name)
				} else {
					vals = vals.Concat(Q[j].Col(name))
				}
			}
			seriesList = append(seriesList, vals)
		}
	}
	return dataframe.New(seriesList...), nil
}
