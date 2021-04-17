package k_anonymity

import (
	"fmt"
)

// 无序属性
type DisorderQuality struct {
	Set []string `json:"set"`
}

type DisorderQualityConfig struct {
	Weight                    float64
	DisorderQualityFuncStruct DisorderQualityFuncStruct
}

type DisorderQualityFuncStruct struct {
	FormatFunc func(DisorderQuality) (string, error)
	MergeFunc  func(...DisorderQuality) (DisorderQuality, error)
}

const DefaultDisorderWeight float64 = 1

func GetDefaultDisorderQualityFuncStruct() DisorderQualityFuncStruct {
	return DisorderQualityFuncStruct{
		DefaultDisorderFormatFunc,
		DefaultDisorderMergeFunc,
	}
}

func DefaultDisorderFormatFunc(d DisorderQuality) (string, error) {
	return fmt.Sprintf("%v", d.Set), nil
}

func DefaultDisorderMergeFunc(vals ...DisorderQuality) (DisorderQuality, error) {
	dis := DisorderQuality{}
	valMap := map[string]struct{}{}
	for i := range vals {
		for _, s := range vals[i].Set {
			valMap[s] = struct{}{}
		}
	}
	for k := range valMap {
		dis.Set = append(dis.Set, k)
	}
	return dis, nil
}
