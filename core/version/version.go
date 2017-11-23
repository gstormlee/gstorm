package version

import "strconv"

type Version struct {
	Major int
	Minor int
}

func NewVersion() {
	v := Version{}
	v.Major = 0
	v.Minor = 21
}

func (v *Version) GetVersion() string {
	str := strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor)
	return str
}
