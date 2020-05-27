package comparator

type comparison int

const (
	LESS_THAN    comparison = -1
	EQUAL        comparison = 0
	GREATER_THAN comparison = 1
)

func Compare(a, b []byte) comparison {
	for i, _ := range a {
		if i+1 > len(b) {
			return GREATER_THAN
		}
		if a[i] > b[i] {
			return GREATER_THAN
		} else if b[i] > a[i] {
			return LESS_THAN
		}
	}

	if len(b) > len(a) {
		return LESS_THAN
	}
	return EQUAL
}
