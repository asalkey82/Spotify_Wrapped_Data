CREATE TABLE IF NOT EXISTS "music_data" (
	"id" INTEGER PRIMARY KEY,
	"song" VARCHAR NOT NULL,
	"album" VARCHAR NOT NULL,
	"artist" VARCHAR NOT NULL,
	"date" TEXT NOT NULL,
	"time_listened" INTEGER NOT NULL,
	"reason_end" VARCHAR NOT NULL,
	"song_id" VARCHAR NOT NULL
)