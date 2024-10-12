package slicex

import (
	"cmp"
	"slices"
)

func InsertBinary[T cmp.Ordered](s []T, v T, maxLength int) (result []T, inserted int) {
	defer func() {
		if r := recover(); r != nil {
			result = s
			inserted = -1
		}
	}()
	idx, _ := slices.BinarySearch(s, v)
	switch idx {
	case len(s):
		if len(s) < maxLength {
			return append(s, v), len(s)
		}
		return s, -1
	default:
		ns := make([]T, len(s)+1)
		copy(ns[:idx], s[:idx])
		ns[idx] = v
		switch {
		case len(s) < maxLength:
			copy(ns[idx+1:], s[idx:])
			return ns, idx
		default:
			copy(ns[idx+1:], s[idx:len(s)-1])
		}
		return ns, idx
	}
}

func InsertAt[T any](s []T, idx int, v T, maxLength int) []T {
	if idx < 0 || idx > len(s) {
		return s
	}
	switch idx {
	case len(s):
		if len(s) < maxLength {
			return append(s, v)
		}
		return append(s[:idx], v)
	default:
		ns := make([]T, len(s)+1)
		copy(ns[:idx], s[:idx])
		ns[idx] = v
		switch {
		case len(s) < maxLength:
			copy(ns[idx+1:], s[idx:])
			return ns
		default:
			copy(ns[idx+1:], s[idx:len(s)-1])
		}
		return ns
	}
}
