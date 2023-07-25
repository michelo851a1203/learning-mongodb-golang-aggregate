package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	ctx := context.Background()
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://michael:secret@localhost:27017/"))
	if err != nil {
		log.Fatalf("mongo start client error : %v\n", err)
		return
	}
	defer func() {

		if mongoClient.Disconnect(ctx); err != nil {
			log.Fatalf("mongo disconnect error : %v\n", err)
			return
		}
	}()

	err = mongoClient.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("connect to mongodb error : %v\n", err)
		return
	}
	demoDatabase := mongoClient.Database("demo")
	ordersCollection := demoDatabase.Collection("orders")

	matchPipeline := bson.D{
		bson.E{
			Key: "$match",
			Value: bson.D{
				bson.E{
					Key:   "status",
					Value: "completed",
				},
			},
		},
	}

	groupPipeline := bson.D{
		bson.E{
			Key: "$group",
			Value: bson.D{
				bson.E{
					Key:   "_id",
					Value: "$name",
				},
				bson.E{
					Key: "total",
					Value: bson.D{
						bson.E{
							Key:   "$sum",
							Value: "$amount",
						},
					},
				},
			},
		},
	}

	sortPipeline := bson.D{
		bson.E{
			Key: "$sort",
			Value: bson.D{
				bson.E{
					Key:   "total",
					Value: -1,
				},
			},
		},
	}

	cursor, err := ordersCollection.Aggregate(ctx, mongo.Pipeline{
		matchPipeline,
		groupPipeline,
		sortPipeline,
	})
	if err != nil {
		log.Fatalf("pipeline error : %v\n", err)
		return
	}
	var result []struct {
		Name  string `bson:"_id"`
		Total int64  `bson:"total"`
	}
	err = cursor.All(ctx, &result)
	if err != nil {
		log.Fatalf("cursor error : %v\n", err)
		return
	}
	fmt.Println(result)
}
