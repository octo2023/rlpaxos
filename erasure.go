package paxi

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

func Splitvalue(value Value, X int, K int) (nums [][]byte, err error) {
	// 数据分10片和校验3片 create an encoder with 10 data shards and 3 parity shards
	enc, err := reedsolomon.New(X, K)
	if err != nil {
		return nil, fmt.Errorf("针对原始文件拆分成数据[%d]块,校验[%d]块失败: %s", X, K, err.Error())
	}

	// 将原始数据按照规定的分片数进行切分
	shards, err := enc.Split(value)
	if err != nil {
		return nil, fmt.Errorf("针对原始文件拆分成数据[%d]块,校验[%d]块失败,%s", X, K, err.Error())
	}

	// 编码校验块
	if err = enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("编码校验块失败,%s", err.Error())
	}
	return shards, nil
}

func recoverdata(data [][]byte) (nums [][]byte, err error) {
	enc, err := reedsolomon.New(3, 2)
	if err != nil {
		return nil, fmt.Errorf("创建数据分片和校验分片失败: %s", err.Error())
	}
	err = enc.Reconstruct(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to encode data:", err)
	}
	return data, nil
}

// func recoverFile() error {

// 	// 数据分10片和校验3片
// 	enc, err := reedsolomon.New(X, K)
// 	if err != nil {
// 		return fmt.Errorf("创建数据分片和校验分片失败,%s", err.Error())
// 	}

// 	shards := make([][]byte, dataShard+parityShard)
// 	for i := range shards {
// 		splitName := fmt.Sprintf("%ssplit%010d", desDir, i)
// 		// 不管文件是否存在，需要保留原先的顺序
// 		if shards[i], err = ioutil.ReadFile(splitName); err != nil {
// 			fmt.Printf("读取文件[%s]失败,%s\n", splitName, err.Error())
// 		}
// 		fmt.Println(splitName)
// 	}

// 	ok, err := enc.Verify(shards)
// 	if ok {
// 		fmt.Println("非常好,数据块和校验块都完整")
// 	} else {
// 		if err = enc.Reconstruct(shards); err != nil {
// 			return fmt.Errorf("重建其他损坏的分片失败,%s", err.Error())
// 		}

// 		if ok, err = enc.Verify(shards); err != nil {
// 			return fmt.Errorf("数据块校验失败2,%s", err.Error())
// 		}
// 		if !ok {
// 			return fmt.Errorf("重建其他损坏的分片后数据还是不完整,文件损坏")
// 		}

// 	}
// 	f, err := os.Create(recoverName)
// 	if err != nil {
// 		return fmt.Errorf("创建还原文件[%s]失败,%s", recoverName, err.Error())
// 	}
// 	// 这部分的大小决定了还原后的大小和原先的是不是一致的,不然使用md5比对或者大小都是不一样的
// 	// 实际生产需要一开始就拆分文件时候就记录总的大小
// 	//if err = enc.Join(f, shards, len(shards[0])*dataShards); err != nil {
// 	// _, ln, err := file.GetFileLenAndMd5(srcFile)
// 	// if err != nil {
// 	// 	return fmt.Errorf("计算原始文件[%s]大小失败,%s", srcFile, err.Error())
// 	// }
// 	if err = enc.Join(f, shards, len(shards[0])*dataShard); err != nil {
// 		return fmt.Errorf("写还原文件[%s]失败,%s", recoverFile(), err.Error())
// 	}
// 	return nil
// }
