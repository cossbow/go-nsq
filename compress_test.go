package nsq

import (
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func getTestData() ([]byte, error) {
	f, er := os.Open("compress.go")
	if nil != er {
		return nil, er
	}
	return ioutil.ReadAll(f)
}

func testCompress(compress CompressType, data []byte) error {
	ed, er := compressBytes(compress, data)
	if nil != er {
		return er
	}

	cd, er := decompressBytes(compress, ed)
	if nil != er {
		return er
	}

	if reflect.DeepEqual(data, cd) {
		return nil
	}

	return errors.New("compress error")
}

func Test_compressBytes(t *testing.T) {
	data, er := getTestData()
	if nil != er {
		t.Fail()
	}

	compresses := []CompressType{CompressNon, CompressSnappy, CompressDeflate}

	for _, compress := range compresses {
		er := testCompress(compress, data)
		if nil != er {
			t.Logf("compress %d: %v", compress, er)
			t.Fail()
		}
	}

}
