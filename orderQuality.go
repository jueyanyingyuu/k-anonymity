package k_anonymity

import (
	"fmt"
	"math"
	"strconv"
)

// 有序属性
type OrderQuality struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

type OrderQualityConfig struct {
	Weight                 float64
	OrderQualityFuncStruct OrderQualityFuncStruct
}

type OrderQualityFuncStruct struct {
	FormatFunc func(OrderQuality) (string, error)
	ExpandFunc func(...OrderQuality) (OrderQuality, error)
	DValueFunc func(OrderQuality) (float64, error)
}

const DefaultOrderWeight float64 = 1

func GetDefaultInt64OrderQualityFuncStruct() OrderQualityFuncStruct {
	return OrderQualityFuncStruct{
		DefaultInt64OrderFormatFunc,
		DefaultInt64OrderExpandFunc,
		DefaultInt64OrderDValueFunc,
	}
}

func DefaultInt64OrderFormatFunc(o OrderQuality) (string, error) {
	if o.Min == o.Max {
		return fmt.Sprintf("%s", o.Min), nil
	}
	return fmt.Sprintf("[%s-%s]", o.Min, o.Max), nil
}

func DefaultInt64OrderExpandFunc(vals ...OrderQuality) (OrderQuality, error) {
	expand := OrderQuality{}
	if len(vals) == 0 {
		return expand, fmt.Errorf("有序属性参数数组为空")
	}
	var min, max = int64(math.MaxInt64), int64(math.MinInt64)
	for i := range vals {
		minTmp, err := strconv.ParseInt(vals[i].Min, 10, 64)
		if err != nil {
			return expand, err
		}
		if minTmp < min {
			min = minTmp
		}
		maxTmp, err := strconv.ParseInt(vals[i].Max, 10, 64)
		if err != nil {
			return expand, err
		}
		if maxTmp > max {
			max = maxTmp
		}
	}
	expand.Min = strconv.FormatInt(min, 10)
	expand.Max = strconv.FormatInt(max, 10)
	return expand, nil
}

func DefaultInt64OrderDValueFunc(o OrderQuality) (float64, error) {
	min, err := strconv.ParseInt(o.Min, 10, 64)
	if err != nil {
		return 0, err
	}
	max, err := strconv.ParseInt(o.Max, 10, 64)
	if err != nil {
		return 0, err
	}
	return float64(max - min), nil
}

func GetDefaultFloat64OrderQualityFuncStruct() OrderQualityFuncStruct {
	return OrderQualityFuncStruct{
		DefaultFloat64OrderFormatFunc,
		DefaultFloat64OrderExpandFunc,
		DefaultFloat64OrderDValueFunc,
	}
}

func DefaultFloat64OrderFormatFunc(o OrderQuality) (string, error) {
	if o.Min == o.Max {
		return fmt.Sprintf("%s", o.Min), nil
	}
	return fmt.Sprintf("[%s-%s]", o.Min, o.Max), nil
}

func DefaultFloat64OrderExpandFunc(vals ...OrderQuality) (OrderQuality, error) {
	expand := OrderQuality{}
	if len(vals) == 0 {
		return expand, fmt.Errorf("有序属性参数数组为空")
	}
	var min, max = math.MaxFloat64, -math.MaxFloat64
	for i := range vals {
		minTmp, err := strconv.ParseFloat(vals[i].Min, 64)
		if err != nil {
			return expand, err
		}
		if minTmp < min {
			min = minTmp
		}
		maxTmp, err := strconv.ParseFloat(vals[i].Max, 64)
		if err != nil {
			return expand, err
		}
		if maxTmp > max {
			max = maxTmp
		}
	}
	expand.Min = strconv.FormatFloat(min, 'e', -1, 10)
	expand.Max = strconv.FormatFloat(max, 'e', -1, 10)
	return expand, nil
}

func DefaultFloat64OrderDValueFunc(o OrderQuality) (float64, error) {
	min, err := strconv.ParseFloat(o.Min, 64)
	if err != nil {
		return 0, err
	}
	max, err := strconv.ParseFloat(o.Max, 64)
	if err != nil {
		return 0, err
	}
	return max - min, nil
}