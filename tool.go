package k_anonymity

import (
	"encoding/json"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func generalize(d dataframe.DataFrame, config DataSetConfig) (dataframe.DataFrame, error) {
	names := d.Names()
	var seriesList []series.Series
	for _, name := range names {
		col := d.Col(name)
		if orderConfig, ok := config.order[name]; ok {
			var orderElemList []OrderQuality
			for i := 0; i < col.Len(); i++ {
				val := OrderQuality{}
				valStr := col.Val(i).(string)
				_ = json.Unmarshal([]byte(valStr), &val)
				orderElemList = append(orderElemList, val)
			}
			order, err := orderConfig.OrderQualityFuncStruct.ExpandFunc(orderElemList...)
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			orderStr, _ := json.Marshal(order)
			seriesList = append(seriesList, series.New(string(orderStr), series.String, name))
		} else if disorderConfig, ok := config.disorder[name]; ok {
			var disorderElemList []DisorderQuality
			for i := 0; i < col.Len(); i++ {
				val := DisorderQuality{}
				valStr := col.Val(i).(string)
				_ = json.Unmarshal([]byte(valStr), &val)
				disorderElemList = append(disorderElemList, val)
			}
			disorder, err := disorderConfig.DisorderQualityFuncStruct.MergeFunc(disorderElemList...)
			if err != nil {
				return dataframe.DataFrame{}, err
			}
			disorderStr, _ := json.Marshal(disorder)
			seriesList = append(seriesList, series.New(string(disorderStr), series.String, name))
		} else {
			seriesList = append(seriesList, series.New(nil, col.Type(), name))
		}
	}
	return dataframe.New(seriesList...), nil
}

// 有序属性的信息损失
func lossOrderQuality(o, os OrderQuality, dValue func(OrderQuality) (float64, error)) (float64, error) {
	od, err := dValue(o)
	if err != nil {
		return 0, err
	}
	osd, err := dValue(os)
	if err != nil {
		return 0, err
	}
	return (osd + 1) / (od + 1), nil
}

// 无序属性的信息损失
func lossDisorderQuality(d, ds DisorderQuality) (float64, error) {
	if len(d.Set) == 0 || len(ds.Set) == 0 {
		return 0, fmt.Errorf("无序属性不应当为空")
	}
	return float64(len(d.Set)) / float64(len(ds.Set)), nil
}

// 数据集(元组)信息损失函数，返回原本数据集s与概化后数据集ds的信息损失。分为有序属性和无需属性两部分
func loss(a, b dataframe.DataFrame, config DataSetConfig) (float64, error) {
	//todo check
	loss := float64(0)
	for i := 0; i < a.Nrow(); i++ {
		tupleLoss := float64(0)
		for j := 0; j < a.Ncol(); j++ {
			seriesName := a.Names()[j]
			if _, ok := config.order[seriesName]; ok {
				oStr := a.Elem(i, j).String()
				osStr := b.Elem(i, j).String()
				order := OrderQuality{}
				orderSharp := OrderQuality{}

				_ = json.Unmarshal([]byte(oStr), &order)
				_ = json.Unmarshal([]byte(osStr), &orderSharp)

				f := config.order[seriesName].OrderQualityFuncStruct.DValueFunc
				newLoss, err := lossOrderQuality(order, orderSharp, f)
				if err != nil {
					return 0, err
				}
				tupleLoss += config.order[seriesName].Weight * newLoss

			} else if _, ok := config.disorder[seriesName]; ok {
				dStr := a.Elem(i, j).String()
				dsStr := b.Elem(i, j).String()
				disorder := DisorderQuality{}
				disorderSharp := DisorderQuality{}

				_ = json.Unmarshal([]byte(dStr), &disorder)
				_ = json.Unmarshal([]byte(dsStr), &disorderSharp)

				newLoss, err := lossDisorderQuality(disorder, disorderSharp)
				if err != nil {
					return 0, err
				}
				tupleLoss += config.disorder[seriesName].Weight * newLoss
			}
		}
		loss += tupleLoss
	}
	return loss, nil
}

//数据集之间的距离
func distance2(a, b dataframe.DataFrame, config DataSetConfig) (float64, error) {
	Na := a.Nrow()
	Nb := b.Nrow()
	ta, err := generalize(a.Copy(), config)
	if err != nil {
		return 0, err
	}
	tb, err := generalize(b.Copy(), config)
	if err != nil {
		return 0, err
	}
	return distance4(Na, Nb, ta, tb, config)
}

//数据集之间的距离
func distance4(Na, Nb int, ta, tb dataframe.DataFrame, config DataSetConfig) (float64, error) {
	ts, err := generalize(ta.RBind(tb), config)
	if err != nil {
		return 0, err
	}
	tas, err := generalize(ta.RBind(ts), config)
	if err != nil {
		return 0, err
	}
	tbs, err := generalize(tb.RBind(ts), config)
	if err != nil {
		return 0, err
	}
	lossAAs, err := loss(ta, tas, config)
	if err != nil {
		return 0, err
	}
	lossSAs, err := loss(ts, tas, config)
	if err != nil {
		return 0, err
	}
	lossBBs, err := loss(tb, tbs, config)
	if err != nil {
		return 0, err
	}
	lossSBs, err := loss(ts, tbs, config)
	if err != nil {
		return 0, err
	}
	return float64(Na)*(lossAAs+lossSAs) + float64(Nb)*(lossBBs+lossSBs), nil
}

func removeRow(d dataframe.DataFrame, idx int) dataframe.DataFrame {
	idxes := make([]bool, d.Nrow(), d.Nrow())
	for i := range idxes {
		if i != idx {
			idxes[i] = true
		}
	}
	return d.Subset(idxes)
}

func lCheck(a dataframe.DataFrame, config DataSetConfig) bool {
	col := a.Col(config.s)
	valMap := map[interface{}]struct{}{}
	for i := 0; i < col.Len(); i++ {
		valMap[col.Val(i)] = struct{}{}
		if len(valMap) >= config.l {
			break
		}
	}
	if len(valMap) >= config.l {
		return true
	} else {
		return false
	}
}
