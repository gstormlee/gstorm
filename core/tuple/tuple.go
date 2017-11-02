package tuple

// IID interface
type IID interface {
	GetID() string
	SetID(strID string)
}

type IData interface {
	IID
	GetCurrentID() string
	SetCurrentID(strId string)
}

// SentenceValue struce
type SentenceValue struct {
	Sentence string
	ID
}

type NsqType struct {
	ID
	Msg string
}
type IWordValue interface {
	GetWord() string
}

type WordValue struct {
	Word string
	ID
}

// ID struct
type ID struct {
	ID        string
	CurrentID string
}

// GetID func
func (id *ID) GetID() string {
	return id.ID
}

// SetID func
func (id *ID) SetID(strID string) {
	id.ID = strID
}

// SetCurrentID func
func (id *ID) SetCurrentID(strID string) {
	id.CurrentID = strID
}

func (id *ID) GetCurrentID() string {
	return id.CurrentID
}

func (w *WordValue) GetWord() string {
	return w.Word
}
