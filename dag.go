package merkledag

import "hash"

const (
	K = 1 << 10
	BLOCK_SIZE = 256 * K
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}
 


func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	switch node.Type() {
	case File:
		StoreFile(store, node, h);
		break;
	case Dir:
		StoreDir(store, node, h);
		break;
	}
	
	return nil
}

func StoreFile(store KVStore, node File , h hash.Hash) ([]byte,[]byte) {
	data := file.Bytes() // 假设File接口有Bytes()方法返回文件内容  
	if len(data) <= BLOCK_SIZE {  
		// 小文件直接存储  
		h.Write(data)
		key := h.Sum(data)  
		store.Put(key, data)  
		return key, []byte("blob")  
	}  

	var merkleRoot []byte  
	var hashes [][]byte  
	offset := 0 
	for offset < len(data) {  
		end := offset + BLOCK_SIZE  
		if end > len(data) {  
			end = len(data)  
		}
		block := data[offset:end]
		h.Reset()
		h.Write(block) 
		blockHash := h.Sum(block)  
		hashes = append(hashes, blockHash) 
		key := blockHash 
		store.Put(key, block)
		offset = end  
	}
	var merkleTree [][]byte
	for len(hashes) > 1 {  
        if len(hashes)%2 != 0 {  
			// 如果哈希数量是奇数，复制最后一个哈希
            hashes = append(hashes, hashes[len(hashes)-1])   
        } 
		pairHashes := make([][]byte, 0, len(hashes)/2)
		for i := 0; i < len(hashes); i += 2 {
			// 克隆哈希实例以避免状态污染
			pairHasher := h.Clone()   
            pairHasher.Write(hashes[i])  
            pairHasher.Write(hashes[i+1])
			pairHash := pairHasher.Sum(pairHasher)
			pairHashes = append(pairHashes, pairHash)
		}
		hashes = pairHashes
} 
	// 克隆哈希实例以计算最终的Merkle Roo
	merkleRoot = h.Clone() t  
	merkleRoot.Write(hashes[0])  
	merkleRootHash := merkleRoot.Sum(merkleRoot)

	return merkleRootHash, []byte("list")
}

func StoreDir(store KVStore, dir Dir , h hash.Hash) []byte {
	

	tree := Object{
		Links: make([]Links, 0),
		Data:  make([][]byte, 0),

	}
	it := dir.It()
	for it.Next() {
		node := it.Node()
		switch node.Type() {
		case File:
			merkleRoot, _ := StoreFile(store, node, h)
			tree.Links = append(tree.Links, Link{Name: node.Name(), Hash: merkleRoot, Size: node.Size()})
			break;
		case Dir:
			merkleRoot := StoreDir(store, node, h)
			tree.Links = append(tree.Links, Link{Name: node.Name(), Hash: merkleRoot, Size: node.Size()})
			break;
		}
	}
	h.Reset()
	h.Write(tree)
	merkleRoot := h.Sum(tree)
	store.Put(merkleRoot, tree)
	return merkleRoot

}


