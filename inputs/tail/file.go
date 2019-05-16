package tail

import (
	"os"
)

type File struct {
	File     *os.File
	FileInfo os.FileInfo
	Path     string
}

