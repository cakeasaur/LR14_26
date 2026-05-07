package main

import (
	"reflect"
	"testing"
)

// computeShard — pure-функция, тестируется без etcd.

func TestComputeShard_SingleNode(t *testing.T) {
	regions := []Region{
		{Name: "A", FederalDistrict: "ЦФО"},
		{Name: "B", FederalDistrict: "ЦФО"},
		{Name: "C", FederalDistrict: "ЦФО"},
	}
	shard, idx, peers := computeShard(regions, []string{"node1"}, "node1")

	if idx != 0 {
		t.Errorf("idx = %d, ожидался 0", idx)
	}
	if !reflect.DeepEqual(peers, []string{"node1"}) {
		t.Errorf("peers = %v", peers)
	}
	if len(shard) != 3 {
		t.Errorf("одиночный сборщик должен взять все 3 региона, получил %d", len(shard))
	}
}

func TestComputeShard_TwoNodes_DeterministicSplit(t *testing.T) {
	regions := []Region{
		{Name: "A"}, {Name: "B"}, {Name: "C"}, {Name: "D"}, {Name: "E"},
	}
	// hostnames в произвольном порядке — функция отсортирует
	peers := []string{"node-z", "node-a"}

	shardA, idxA, _ := computeShard(regions, peers, "node-a")
	shardZ, idxZ, _ := computeShard(regions, peers, "node-z")

	// После sort: ["node-a", "node-z"] → node-a имеет индекс 0
	if idxA != 0 || idxZ != 1 {
		t.Errorf("idxA=%d idxZ=%d (ожидалось 0 и 1)", idxA, idxZ)
	}

	// node-a берёт регионы 0, 2, 4 (i%2==0); node-z — 1, 3
	wantA := []Region{{Name: "A"}, {Name: "C"}, {Name: "E"}}
	wantZ := []Region{{Name: "B"}, {Name: "D"}}
	if !reflect.DeepEqual(shardA, wantA) {
		t.Errorf("shardA = %v, want %v", shardA, wantA)
	}
	if !reflect.DeepEqual(shardZ, wantZ) {
		t.Errorf("shardZ = %v, want %v", shardZ, wantZ)
	}

	// объединение шардов = все регионы (нет потерь и дублей)
	combined := append([]Region{}, shardA...)
	combined = append(combined, shardZ...)
	if len(combined) != len(regions) {
		t.Errorf("суммарно %d регионов, ожидалось %d", len(combined), len(regions))
	}
}

func TestComputeShard_HostnameNotInPeers_TakesAll(t *testing.T) {
	regions := []Region{{Name: "A"}, {Name: "B"}}
	shard, idx, _ := computeShard(regions, []string{"other-1", "other-2"}, "ghost")

	if idx != -1 {
		t.Errorf("idx = %d, ожидался -1 (хост не в списке)", idx)
	}
	if len(shard) != len(regions) {
		t.Error("при отсутствии в peers должны взяться все регионы")
	}
}

func TestComputeShard_ThreeNodes_NoOverlap(t *testing.T) {
	// 84 региона, как в реальном проекте, на 3 ноды
	regions := make([]Region, 84)
	for i := range regions {
		regions[i] = Region{Name: string(rune('A' + i%26))}
	}
	peers := []string{"n1", "n2", "n3"}

	var allShards []Region
	for _, p := range peers {
		s, _, _ := computeShard(regions, peers, p)
		allShards = append(allShards, s...)
		// каждая нода должна получить ровно 28 регионов (84/3)
		if len(s) != 28 {
			t.Errorf("node %s: %d регионов, ожидалось 28", p, len(s))
		}
	}
	if len(allShards) != 84 {
		t.Errorf("суммарно %d регионов, ожидалось 84", len(allShards))
	}
}
