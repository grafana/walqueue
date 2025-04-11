package types

type Data struct {
	Meta map[string]string
	Data []byte
}

type DataHandle struct {
	Pop  func() (map[string]string, []byte, error)
	Name string
}
