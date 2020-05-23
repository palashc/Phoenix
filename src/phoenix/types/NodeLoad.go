package types

type NodeLoad []int
type NodeLoadSeq []*NodeLoad

const TARGET_INDEX = 0
const LOAD_INDEX = 1

func (s NodeLoadSeq) Len() int           { return len(s) }
func (s NodeLoadSeq) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s NodeLoadSeq) Less(i, j int) bool { return (*s[i])[LOAD_INDEX] < (*s[j])[LOAD_INDEX] }
