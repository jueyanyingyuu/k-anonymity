package k_anonymity

// 数据集设置
type DataSetConfig struct {
	// 多样化模型参数
	l int
	//// 准标识符属性名称
	//qi map[string]struct{}
	// 敏感属性名称
	s string
	// 有序属性名称,以及构造方法
	order map[string]*OrderQualityConfig
	// 无序属性名称,以及构造方法
	disorder map[string]*DisorderQualityConfig
}

func NewDataSetConfig(l int, s string, order, disorder []string) *DataSetConfig {
	dataSetConfig := &DataSetConfig{
		l:        l,
		s:        s,
		order:    map[string]*OrderQualityConfig{},
		disorder: map[string]*DisorderQualityConfig{},
	}
	for _, v := range order {
		dataSetConfig.order[v] = &OrderQualityConfig{
			Weight:                 DefaultOrderWeight,
			OrderQualityFuncStruct: GetDefaultInt64OrderQualityFuncStruct(),
		}
	}
	for _, v := range disorder {
		dataSetConfig.disorder[v] = &DisorderQualityConfig{
			Weight:                    DefaultDisorderWeight,
			DisorderQualityFuncStruct: GetDefaultDisorderQualityFuncStruct(),
		}
	}
	return dataSetConfig
}

func (c *DataSetConfig) SetNewOrderQualityWeight(orderName string, weight float64) {
	c.order[orderName].Weight = weight
}

func (c *DataSetConfig) SetNewDisorderQualityWeight(disorderName string, weight float64) {
	c.disorder[disorderName].Weight = weight
}

func (c *DataSetConfig) SetNewOrderQualityFuncStruct(orderName string, s OrderQualityFuncStruct) {
	c.order[orderName].OrderQualityFuncStruct = s
}

func (c *DataSetConfig) SetNewDisorderQualityConstructFunc(disorderName string, s DisorderQualityFuncStruct) {
	c.disorder[disorderName].DisorderQualityFuncStruct = s
}

func (c *DataSetConfig) Copy() DataSetConfig {
	var s = c.s
	var order, disorder []string
	for k := range c.order {
		order = append(order, k)
	}
	for k := range c.disorder {
		disorder = append(disorder, k)
	}
	newConfig := NewDataSetConfig(c.l, s, order, disorder)
	for k, v := range c.order {
		newConfig.SetNewOrderQualityFuncStruct(k, v.OrderQualityFuncStruct)
		newConfig.SetNewOrderQualityWeight(k, v.Weight)
	}
	for k, v := range c.disorder {
		newConfig.SetNewDisorderQualityConstructFunc(k, v.DisorderQualityFuncStruct)
		newConfig.SetNewDisorderQualityWeight(k, v.Weight)
	}
	return *newConfig
}
