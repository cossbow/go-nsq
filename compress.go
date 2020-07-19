package nsq

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"io/ioutil"
)

//
func compressOut(compress CompressType, out io.WriteCloser) io.WriteCloser {
	switch compress {
	case CompressNon:
		return out
	case CompressSnappy:
		return snappy.NewBufferedWriter(out)
	case CompressDeflate:
		return zlib.NewWriter(out)
	default:
		panic(fmt.Sprintf("invalid CompressType: %d", compress))
	}
}

//
func decompressIn(compress CompressType, in io.Reader) (io.Reader, error) {
	switch compress {
	case CompressNon:
		return in, nil
	case CompressSnappy:
		return snappy.NewReader(in), nil
	case CompressDeflate:
		return zlib.NewReader(in)
	default:
		panic(fmt.Sprintf("invalid CompressType: %d", compress))
	}
}

//
type closeBuffer struct {
	bytes.Buffer
}

func (c *closeBuffer) Close() error {
	return nil
}

//
func compressBytes(compress CompressType, s []byte) ([]byte, error) {
	if CompressNon == compress {
		return s, nil
	}
	buf := closeBuffer{}
	w := compressOut(compress, &buf)
	_, er := w.Write(s)
	if nil != er {
		return nil, er
	}
	er = w.Close()
	if nil != er {
		return nil, er
	}
	return buf.Bytes(), nil
}

//
func decompressBytes(compress CompressType, s []byte) ([]byte, error) {
	if CompressNon == compress {
		return s, nil
	}
	br := bytes.NewReader(s)
	r, er := decompressIn(compress, br)
	if nil != er {
		return nil, er
	}
	return ioutil.ReadAll(r)
}
