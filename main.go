package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	if os.Getenv("EXPIRE_TIME") == "" {
		os.Setenv("EXPIRE_TIME", "120000000000")
	} else {
		os.Setenv("EXPIRE_TIME", os.Getenv("EXPIRE_TIME")+"000000000")
	}
	if os.Getenv("MONGO_URL") == "" {
		os.Setenv("MONGO_URL", "docker.rutt.io")
	}
	if os.Getenv("AMQP_URL") == "" {
		os.Setenv("AMQP_URL", "amqp://mqadmin:mqadmin@dev-rabbit-1//")
	}
	if os.Getenv("REDIS_URL") == "" {
		os.Setenv("REDIS_URL", "docker:6379")
	}

	log.Println("Connecting redis at " + os.Getenv("REDIS_URL"))
	red := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URL"),
		Password: "",
		DB:       0,
	})
	_, err := red.Ping().Result()
	failOnError(err, "Failed to connect to redis")

	log.Println("Connecting mongo at " + os.Getenv("MONGO_URL"))
	session, err := mgo.Dial(os.Getenv("MONGO_URL"))
	failOnError(err, "Failed to connect to mongo")
	defer session.Close()

	log.Println("Connecting rabbit at " + os.Getenv("AMQP_URL"))
	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	defer conn.Close()

	log.Println("Setting collection...")
	c := session.DB("rutt").C("systems")

	log.Println("Setting router...")
	router := mux.NewRouter()
	router.HandleFunc("/", indexGet).Methods("Get")
	router.HandleFunc("/publish/{systemId}/{key}", func(w http.ResponseWriter, r *http.Request) {
		publish(w, r, c, ch)
	}).Methods("Post")
	router.HandleFunc("/consume/{systemId}", func(w http.ResponseWriter, r *http.Request) {
		consume(w, r, c, red)
	}).Methods("Get")
	router.HandleFunc("/ack/{messageId}", func(w http.ResponseWriter, r *http.Request) {
		ack(w, r, c, red)
	}).Methods("Get")
	router.HandleFunc("/nack/{messageId}/{requeue}", func(w http.ResponseWriter, r *http.Request) {
		nack(w, r, c, red)
	}).Methods("Get")
	router.HandleFunc("/config/{systemId}", func(w http.ResponseWriter, r *http.Request) {
		config(w, r, c)
	}).Methods("Get")

	log.Println("Listening 8080...")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func indexGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "OK"}`))
}

func publish(w http.ResponseWriter, r *http.Request, c *mgo.Collection, ch *amqp.Channel) {
	params := mux.Vars(r)
	token := extractToken(r)

	var result []map[string]string
	err := c.Find(bson.M{"systemId": params["systemId"], "publishToken": token}).Select(bson.M{"_id": 0, "systemId": 1}).All(&result)
	failOnError(err, "Failed to get config")

	if result == nil {
		http.Error(w, "Wrong token", 401)
	} else {
		if len(result) == 1 {
			var u1, u4, u5 uuid.UUID
			u1, err = uuid.NewV1()
			u4, err = uuid.NewV4()
			u5 = uuid.NewV5(u4, u1.String())
			failOnError(err, "Failed generating UUID")

			ret := map[string]string{
				"messageId": "",
			}
			ret["messageId"] = u5.String()

			body, _ := ioutil.ReadAll(r.Body)

			/*
				// MOVE THIS TO DIRECTOR
				err = ch.ExchangeDeclare(params["systemId"], "topic", true, false, false, false, nil)
				failOnError(err, "Failed to declare exchange")

				_, err = ch.QueueDeclare(params["systemId"], true, false, false, false, nil)
				failOnError(err, "Failed to declare queue")

				err = ch.QueueBind(params["systemId"], "#", params["systemId"], false, nil)
				failOnError(err, "Failed to bind queue")
			*/

			err = ch.Publish(
				params["systemId"],
				params["key"],
				false,
				false,
				amqp.Publishing{
					DeliveryMode:  amqp.Persistent,
					Timestamp:     time.Now(),
					ContentType:   "application/json",
					CorrelationId: u5.String(),
					Body:          []byte(body),
				})
			failOnError(err, "Failed to publish a message")

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ret)
		}
	}
}

func consume(w http.ResponseWriter, r *http.Request, c *mgo.Collection, red *redis.Client) {
	params := mux.Vars(r)
	token := extractToken(r)

	var result []map[string]string
	err := c.Find(bson.M{"systemId": params["systemId"], "publishToken": token}).Select(bson.M{"_id": 0, "systemId": 1}).All(&result)
	failOnError(err, "Failed to get config")

	if result == nil {
		http.Error(w, "Wrong token", 401)
	} else {
		if len(result) == 1 {
			pipe := red.TxPipeline()
			key := params["systemId"] + ".outgoing"
			res := pipe.ZRangeByScoreWithScores(key, redis.ZRangeBy{
				Min:    "-inf",
				Max:    "+inf",
				Offset: 0,
				Count:  1,
			})
			pipe.ZRemRangeByRank(key, 0, 0)
			_, err := pipe.Exec()
			failOnError(err, "Failed consuming message")

			if len(res.Val()) > 0 {
				messageID := res.Val()[0].Member.(string)
				res := red.HGetAll(messageID)

				if res != nil {
					message := res.Val()
					pipe := red.TxPipeline()
					pipe.HMSet(messageID+"_clone", map[string]interface{}{
						"message":       message["message"],
						"correlationId": message["correlationId"],
						"queue":         key,
						"publishTime":   message["publishTime"],
					})
					exp, _ := strconv.ParseUint(os.Getenv("EXPIRE_TIME"), 10, 64)

					pipe.Expire(messageID, time.Duration(exp))
					_, err = pipe.Exec()
					failOnError(err, "Failed consuming message")
					currentTime := time.Now().UnixNano() / 1000000
					logDestinationUpdated(message["correlationId"], params["systemId"], messageID, "timeConsumed", strconv.FormatInt(currentTime, 10))
					w.Header().Set("Message-Id", messageID)
					w.Header().Set("Correlation-Id", message["correlationId"])
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte(message["message"]))
				} else {
					failOnError(err, "Failed consuming message")
				}
			} else {
				http.Error(w, "No more messages", 404)
			}
		}
	}
}

func ack(w http.ResponseWriter, r *http.Request, c *mgo.Collection, red *redis.Client) {
	params := mux.Vars(r)
	token := extractToken(r)

	res := red.HGetAll(params["messageId"])

	if len(res.Val()) > 0 {
		message := res.Val()
		systemID := strings.Replace(message["queue"], ".outgoing", "", 1)
		var result []map[string]string
		err := c.Find(bson.M{"systemId": systemID, "publishToken": token}).Select(bson.M{"_id": 0, "systemId": 1}).All(&result)
		failOnError(err, "Failed to get config")

		if result == nil {
			http.Error(w, "Wrong token", 401)
		} else {
			pipe := red.TxPipeline()
			pipe.Del(params["messageId"])
			pipe.Del(params["messageId"] + "_clone")
			_, err := pipe.Exec()
			failOnError(err, "Failed acking message")
			currentTime := time.Now().UnixNano() / 1000000
			logDestinationUpdated(message["correlationId"], systemID, params["messageId"], "timeAcked", strconv.FormatInt(currentTime, 10))
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status": "OK"}`))
		}
	} else {
		http.Error(w, "No such message", 404)
	}
}

func nack(w http.ResponseWriter, r *http.Request, c *mgo.Collection, red *redis.Client) {
	params := mux.Vars(r)
	token := extractToken(r)

	res := red.HGetAll(params["messageId"])

	if len(res.Val()) > 0 {
		message := res.Val()
		systemID := strings.Replace(message["queue"], ".outgoing", "", 1)
		var result []map[string]string
		err := c.Find(bson.M{"systemId": systemID, "publishToken": token}).Select(bson.M{"_id": 0, "systemId": 1}).All(&result)
		failOnError(err, "Failed to get config")

		if result == nil {
			http.Error(w, "Wrong token", 401)
		} else {
			var timeRequeue int64
			currentTime := time.Now().UnixNano() / 1000000
			if params["requeue"] == "noblock" {
				timeRequeue = currentTime
			} else {
				timeRequeue, _ = strconv.ParseInt(message["publishTime"], 10, 64)
			}
			pipe := red.TxPipeline()
			pipe.ZAdd(systemID, redis.Z{Score: float64(timeRequeue), Member: params["messageId"]})
			pipe.Del(params["messageId"] + "_clone")
			_, err := pipe.Exec()
			failOnError(err, "Failed acking message")
			logDestinationUpdated(message["correlationId"], systemID, params["messageId"], "timeAcked", strconv.FormatInt(currentTime, 10))

			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status": "OK"}`))
		}
	} else {
		http.Error(w, "No such message", 404)
	}
}

func config(w http.ResponseWriter, r *http.Request, c *mgo.Collection) {
	params := mux.Vars(r)
	token := extractToken(r)

	var result map[string][]map[string]string
	err := c.Find(bson.M{"systemId": params["systemId"], "publishToken": token}).Select(bson.M{"_id": 0, "objectTypes.objectId": 1}).One(&result)
	failOnError(err, "Failed to get config")

	var ret []string
	for _, v := range result["objectTypes"] {
		ret = append(ret, v["objectId"])
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ret)
}

func extractToken(r *http.Request) string {
	token := r.Header.Get("Authorization")
	token = strings.TrimPrefix(token, "Bearer ")
	return token
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func progressOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func logNewMessage(newMessageUID string, systemID string) {
	_, err := http.Get("http://logger:8084/new/" + newMessageUID + "/" + systemID)
	progressOnError(err, "Failed logging new message")
}

func logQueued(newMessageUID string) {
	currentTime := time.Now().UnixNano() / 1000000
	_, err := http.Get("http://logger:8084/queued/" + newMessageUID + "/" + (string)(currentTime))
	progressOnError(err, "Failed logging message queued")
}

func logDestinationUpdated(correlationID string, destinationID string, messageID string, field string, value string) {
	_, err := http.Get("http://logger:8084/destinationUpdated/" + correlationID + "/" + destinationID + "/" + messageID + "/" + field + "/" + value)
	progressOnError(err, "Failed logging new message")
}
