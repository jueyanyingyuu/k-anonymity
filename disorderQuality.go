package k_anonymity

import (
	"encoding/binary"
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
	result := DisorderQuality{}
	buf := []byte(str)
	setLen, length := binary.Uvarint(buf)
	if length <= 0 {
		return result, fmt.Errorf("无法正确解析无序属性")
	}
	buf = buf[length:]

	for i := uint64(0); i < setLen; i++ {
		strLen, length := binary.Uvarint(buf)
		if length <= 0 {
			return result, fmt.Errorf("无法正确解析无序属性")
		}
		buf = buf[length:]
		result.Set = append(result.Set, string(buf[:strLen]))
		buf = buf[strLen:]
	}
	return result, nil
}
func DefaultDisorderMarshalFunc(d DisorderQuality) (string, error) {
	var sliceList [][]byte
	var size int

	setLenBuf := make([]byte, binary.MaxVarintLen64)
	setLen := binary.PutUvarint(setLenBuf, uint64(len(d.Set)))
	size += setLen
	sliceList = append(sliceList, setLenBuf[:setLen])

	for _, v := range d.Set {
		strLenBuf := make([]byte, binary.MaxVarintLen64)
		strLen :=binary.PutUvarint(strLenBuf, uint64(len(v)))
		size += strLen
		size += len(v)
		sliceList = append(sliceList, strLenBuf[:strLen])
		sliceList = append(sliceList, []byte(v))
	}
	slice := make([]byte, 0, size)
	for _, v := range sliceList {
		slice = append(slice, v...)
	}
	return string(slice), nil
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
