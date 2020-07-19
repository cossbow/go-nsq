package main

import (
	"encoding/json"
	"fmt"
	. "github.com/nsqio/go-nsq"
	"io/ioutil"
	"log"
	"math/rand"
	"time"
)

const topic = "test-topic-jjj"

var nullLogger = log.New(ioutil.Discard, "", log.LstdFlags)

//

type DailyConsumerPrice struct {
	RateId    int64     `json:"rateId"`
	Date      time.Time `json:"date"`
	Amount    int32     `json:"amount"`
	Selling   bool      `json:"selling"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Rate struct {
	Id                  int64                `json:"id"`
	RoomTypeId          int64                `json:"roomTypeId"`
	RatePlanId          int64                `json:"ratePlanId"`
	HotelId             int64                `json:"hotelId"`
	Enable              bool                 `json:"enable"`
	DailyConsumerPrices []DailyConsumerPrice `json:"dailyConsumerPrices"`
}

func newRate() *Rate {
	var prices []DailyConsumerPrice
	var N = rand.Intn(32)
	var m = rand.Intn(11) + 1
	var d = rand.Intn(1)
	date0 := time.Date(2020, time.Month(m), d, 0, 0, 0, 0, time.Local).UTC()
	for i := 0; i < N; i++ {
		price := DailyConsumerPrice{}
		price.Date = date0.Add(time.Hour * 24)
		price.Amount = (rand.Int31n(100) + 400) * 100
		price.Selling = rand.Int31n(64)%3 > 0
		prices = append(prices, price)
	}
	return &Rate{
		Id:                  rand.Int63n(999999999),
		RoomTypeId:          rand.Int63n(9999999),
		RatePlanId:          rand.Int63n(8),
		HotelId:             rand.Int63n(199999),
		Enable:              rand.Int()%3 > 0,
		DailyConsumerPrices: prices,
	}
}

func newCompressProducer(compress CompressType) *Producer {
	config := NewConfig()
	config.Compress = compress
	p, er := NewProducer("127.0.0.1:4150", config)
	throwErr(er)
	p.SetLogger(nullLogger, LogLevelInfo)
	return p
}

var producerNon = newCompressProducer(CompressNon)
var producerSnappy = newCompressProducer(CompressSnappy)
var producerDeflate = newCompressProducer(CompressDeflate)

var producers = []*Producer{producerNon, producerSnappy, producerDeflate}

func randProducer() *Producer {
	return producers[rand.Intn(len(producers))]
}

func newConsumer() {
	config := NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, _ := NewConsumer(topic, "golang", config)
	q.SetLogger(nullLogger, LogLevelInfo)

	q.AddHandler(HandlerFunc(func(msg *Message) error {
		rate := Rate{}
		er := json.Unmarshal(msg.Body, &rate)
		throwErr(er)
		fmt.Println("get: ", rate)
		return nil
	}))

	er := q.ConnectToNSQD("127.0.0.1:4150")
	throwErr(er)

}

func main() {
	newConsumer()

	for {
		r := newRate()
		data, er := json.Marshal(r)
		throwErr(er)
		p := randProducer()
		if rand.Int31n(10)%2 == 0 {
			er = p.Publish(topic, data)
		} else {
			er = p.DeferredPublish(topic, time.Second, data)
		}
		throwErr(er)
		time.Sleep(time.Millisecond * 500)
	}
}

func throwErr(er error) {
	if nil != er {
		panic(er)
	}
}
