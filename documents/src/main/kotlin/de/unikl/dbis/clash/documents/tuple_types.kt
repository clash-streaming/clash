package de.unikl.dbis.clash.documents


data class TupleKey1<K1, P>(val key1: K1, val payload: P) {

}

data class TupleKey2<K1, K2, P>(val key1: K1, val key2: K2, val payload: P)  {

}


data class TupleKey1Ts<K1, P>(val key1: K1, val ts: Long, val payload: P) {

}

data class TupleKey2Ts<K1, K2, P>(val key1: K1, val key2: K2, val ts: Long, val payload: P)  {

}
