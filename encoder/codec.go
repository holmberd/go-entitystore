package encoder

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, out any) error
}
