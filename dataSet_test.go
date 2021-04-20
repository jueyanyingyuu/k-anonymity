package k_anonymity

import (
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"math/rand"
	"testing"
)

//func BenchmarkDataSet_LClustering(b *testing.B) {
//	csvStr := `
//性别,年龄,身高,健康状况
//男,中年,172,脑溢血
//男,中年,180,心脏病
//男,少年,174,感冒
//女,少年,165,发烧
//女,老年,155,感冒
//女,老年,162,肺炎
//`
//	df := dataframe.ReadCSV(strings.NewReader(csvStr))
//	config := NewDataSetConfig(2,"健康状况",[]string{"身高"},[]string{"性别","年龄"})
//	dataSet,err := NewDataSet(df,*config)
//	if err != nil {
//		println(err)
//	}
//	clustering, err := dataSet.LClustering()
//	if err != nil {
//		fmt.Println(err)
//	}
//	fmt.Println(clustering)
//}

func BenchmarkDataSet_LClustering(b *testing.B) {
	var rows = 1000
	var seriesList []series.Series

	var areasList = []string{"京","津","冀","晋","内蒙古","辽","吉","黑","沪","苏","浙","皖","闽","赣","鲁","豫","鄂","湘","粤","桂","琼","渝","川","黔","滇","藏","陕","甘","青","宁","新","港","澳"}
	var areas []string
	for i := 0; i< rows;i++ {
		areas = append(areas, areasList[rand.Intn(33)])
	}
	seriesList = append(seriesList, series.New(areas,series.String,"地区"))

	var height []int
	for i := 0; i< rows;i++ {
		height = append(height, rand.Intn(50)+150)
	}
	seriesList = append(seriesList, series.New(height,series.Int,"身高"))


	var s []int
	for i := 0; i< rows;i++ {
		s = append(s, rand.Intn(10))
	}
	seriesList = append(seriesList, series.New(s,series.Int,"分数"))


	df := dataframe.New(seriesList...)
	config := NewDataSetConfig(2, "分数", []string{"身高"}, []string{"地区"})
	dataSet, err := NewDataSet(df, *config)
	if err != nil {
		println(err)
	}
	clustering, err := dataSet.LClustering()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(clustering)
}
