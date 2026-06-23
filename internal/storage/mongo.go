package storage

import (
	"context"

	"gfap/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	client *mongo.Client
	col    *mongo.Collection
}

func NewMongo(uri, db, col string) (*Mongo, error) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	collection := client.Database(db).Collection(col)
	collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "date", Value: 1}},
	})

	return &Mongo{client: client, col: collection}, nil
}

func (m *Mongo) Upsert(ctx context.Context, v model.Video) error {
	_, err := m.col.ReplaceOne(ctx, bson.M{"_id": v.URL}, v, options.Replace().SetUpsert(true))
	return err
}

func (m *Mongo) FindAll(ctx context.Context) ([]model.Video, error) {
	cursor, err := m.col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var videos []model.Video
	err = cursor.All(ctx, &videos)
	return videos, err
}

func (m *Mongo) FindTargets(ctx context.Context) ([]model.Video, error) {
	cursor, err := m.col.Find(ctx, bson.M{"is_target": true})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var videos []model.Video
	err = cursor.All(ctx, &videos)
	return videos, err
}

func (m *Mongo) Drop(ctx context.Context) error {
	return m.col.Drop(ctx)
}

func (m *Mongo) Close() {
	m.client.Disconnect(context.Background())
}

func (m *Mongo) Count(ctx context.Context) (int64, error) {
	return m.col.CountDocuments(ctx, bson.M{})
}
