package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gfap/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Mongo struct {
	client *mongo.Client
	col    *mongo.Collection
}

// RequireEmptyMongoForTest checks every collection before a fresh test starts.
// It creates no indexes and changes no data. The caller must have exclusive
// use of the database; this check is not a lock against concurrent writers.
func RequireEmptyMongoForTest(ctx context.Context, uri, dbName string) (err error) {
	switch dbName {
	case "", "admin", "config", "local":
		return fmt.Errorf("test preflight: invalid application database %q", dbName)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(uri).SetReadPreference(readpref.Primary()))
	if err != nil {
		return fmt.Errorf("test preflight: connect to Mongo: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		if closeErr := client.Disconnect(closeCtx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("test preflight: disconnect Mongo: %w", closeErr))
		}
	}()

	db := client.Database(dbName)
	collections, err := db.ListCollections(ctx, bson.D{},
		options.ListCollections().SetNameOnly(true))
	if err != nil {
		return fmt.Errorf("test preflight: list Mongo collections: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		if closeErr := collections.Close(closeCtx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("test preflight: close collection cursor: %w", closeErr))
		}
	}()

	for collections.Next(ctx) {
		var collection struct {
			Name string `bson:"name"`
		}
		if err := collections.Decode(&collection); err != nil {
			return fmt.Errorf("test preflight: decode collection name: %w", err)
		}
		if collection.Name == "" {
			return errors.New("test preflight: collection name is missing")
		}

		queryErr := db.Collection(collection.Name).FindOne(ctx, bson.D{},
			options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
		switch {
		case errors.Is(queryErr, mongo.ErrNoDocuments):
			continue
		case queryErr != nil:
			return fmt.Errorf("test preflight: inspect %s.%s: %w", dbName, collection.Name, queryErr)
		default:
			return fmt.Errorf("test refused: Mongo database %q contains documents in collection %q",
				dbName, collection.Name)
		}
	}
	if err := collections.Err(); err != nil {
		return fmt.Errorf("test preflight: read collection cursor: %w", err)
	}
	return ctx.Err()
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

// EachVideoURL streams every stored video URL in batches, projecting only
// _id so a corpus of millions never has to be decoded or held in full.
func (m *Mongo) EachVideoURL(ctx context.Context, batch int, fn func([]string) error) error {
	cursor, err := m.col.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	urls := make([]string, 0, batch)
	for cursor.Next(ctx) {
		var doc struct {
			URL string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return err
		}
		urls = append(urls, doc.URL)
		if len(urls) >= batch {
			if err := fn(urls); err != nil {
				return err
			}
			urls = urls[:0]
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	if len(urls) > 0 {
		return fn(urls)
	}
	return nil
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
