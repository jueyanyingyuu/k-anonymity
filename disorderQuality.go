package k_anonymity

import (
	"bytes"
	"fmt"
	"strings"
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
	Unmarshal  func(string) (DisorderQuality, error)
	Marshal    func(DisorderQuality) (string, error)
	FormatFunc func(DisorderQuality) (string, error)
	MergeFunc  func(...DisorderQuality) (DisorderQuality, error)
}

const DefaultDisorderWeight float64 = 1

func GetDefaultDisorderQualityFuncStruct() DisorderQualityFuncStruct {
	return DisorderQualityFuncStruct{
		DefaultDisorderUnmarshalFunc,
		DefaultDisorderMarshalFunc,
		DefaultDisorderFormatFunc,
		DefaultDisorderMergeFunc,
	}
}
func DefaultDisorderUnmarshalFunc(str string) (DisorderQuality, error) {
	vals := strings.Split(str, ",")
	result := DisorderQuality{}
	for _, v := range vals {
		result.Set = append(result.Set, v)
	}
	return result, nil
}
func DefaultDisorderMarshalFunc(d DisorderQuality) (string, error) {
	buf := bytes.Buffer{}
	for i, v := range d.Set {
		buf.WriteString(v)
		if i == len(d.Set)-1 {
			break
		}
		buf.WriteString(",")
	}
	return buf.String(),nil
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
