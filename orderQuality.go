package k_anonymity

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
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
	Unmarshal  func(string) (OrderQuality, error)
	Marshal    func(OrderQuality) (string, error)
	FormatFunc func(OrderQuality) (string, error)
	ExpandFunc func(...OrderQuality) (OrderQuality, error)
	DValueFunc func(OrderQuality) (float64, error)
}

const DefaultOrderWeight float64 = 1

func GetDefaultInt64OrderQualityFuncStruct() OrderQualityFuncStruct {
	return OrderQualityFuncStruct{
		DefaultInt64OrderUnmarshalFunc,
		DefaultInt64OrderMarshalFunc,
		DefaultInt64OrderFormatFunc,
		DefaultInt64OrderExpandFunc,
		DefaultInt64OrderDValueFunc,
	}
}

func DefaultInt64OrderUnmarshalFunc(str string) (OrderQuality, error) {

	result := OrderQuality{}
	buf := []byte(str)

	minLen, length := binary.Uvarint(buf)
	if length <= 0 {
		return result, fmt.Errorf("无法正确解析有序属性")
	}
	buf = buf[length:]
	result.Min = string(buf[:minLen])
	buf = buf[minLen:]

	maxLen, length := binary.Uvarint(buf)
	if length <= 0 {
		return result, fmt.Errorf("无法正确解析有序属性")
	}
	buf = buf[length:]
	result.Max = string(buf[:maxLen])
	buf = buf[maxLen:]

	return result, nil
}

func DefaultInt64OrderMarshalFunc(o OrderQuality) (string, error) {
	minBuf := make([]byte, binary.MaxVarintLen64)
	minLen := binary.PutUvarint(minBuf, uint64(len(o.Min)))

	maxBuf := make([]byte, binary.MaxVarintLen64)
	maxLen := binary.PutUvarint(maxBuf, uint64(len(o.Max)))

	slice := make([]byte, 0, minLen+len(o.Min)+maxLen+len(o.Max))
	slice = append(slice, minBuf[:minLen]...)
	slice = append(slice, []byte(o.Min)...)
	slice = append(slice, maxBuf[:maxLen]...)
	slice = append(slice, []byte(o.Max)...)
	return string(slice), nil
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
		DefaultFloat64OrderUnmarshalFunc,
		DefaultFloat64OrderMarshalFunc,
		DefaultFloat64OrderFormatFunc,
		DefaultFloat64OrderExpandFunc,
		DefaultFloat64OrderDValueFunc,
	}
}

func DefaultFloat64OrderUnmarshalFunc(str string) (OrderQuality, error) {
	vals := strings.Split(str, ",")
	if len(vals) != 2 {
		return OrderQuality{}, fmt.Errorf("无法正确解析有序属性")
	}
	return OrderQuality{
		Min: vals[0],
		Max: vals[1],
	}, nil
}

func DefaultFloat64OrderMarshalFunc(orderQuality OrderQuality) (string, error) {
	buf := bytes.Buffer{}
	buf.WriteString(orderQuality.Min)
	buf.WriteString(",")
	buf.WriteString(orderQuality.Max)
	return buf.String(), nil
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
