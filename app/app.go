package app

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/ofjangra/sqsURLProducer/config"
	"github.com/ofjangra/sqsURLProducer/models"
	"gorm.io/gorm"
)

var db *gorm.DB

func InitApp() {
	envLoadErr := godotenv.Load(".env")

	if envLoadErr != nil {
		log.Fatal("Failed to load environment variables")
	}
	var err error
	db, err = config.DBConnection(&config.DBConfig{
		Host:     os.Getenv("DB_HOST"),
		DBName:   os.Getenv("DB_NAME"),
		Port:     os.Getenv("DB_PORT"),
		Password: os.Getenv("DB_PASSWORD"),
		User:     os.Getenv("DB_USER"),
	})

	if err != nil {
		fmt.Println("Db connection error:", err)
	}

	fmt.Println("Database connected")

	db.AutoMigrate(&models.URLs{})
}

func GetDB() *gorm.DB {
	return db
}
