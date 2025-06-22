package writeresult

import (
	"fmt"
	"os"
	"sort"
)

func ToFile(filename string, res map[string]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	keys := make([]string, 0, len(res))
	for k := range res {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	_, err = file.WriteString("{")
	if err != nil {
		return err
	}

	for i, k := range keys {
		v := res[k]
		entry := fmt.Sprintf("%s=%s", k, v)
		if i != 0 {
			entry = ", " + entry
		}
		if _, err := file.WriteString(entry); err != nil {
			return err
		}
	}

	_, err = file.WriteString("}\n")
	return err
}
