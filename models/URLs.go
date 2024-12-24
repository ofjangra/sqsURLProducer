package models

type URLs struct {
	ID  uint   `json:"id" gorm:"column:id; primary_key; autoIncrement"`
	URL string `json:"url" gorm:"column:url; not null"`
}
