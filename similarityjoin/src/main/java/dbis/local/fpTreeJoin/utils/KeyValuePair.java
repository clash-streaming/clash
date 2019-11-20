package dbis.local.fpTreeJoin.utils;

import java.util.Objects;

public class KeyValuePair {
  private String key;
  private String value;
  public int numOfDocsForKey;

  public KeyValuePair(String key, String value){
    this.key = key.trim();
    this.value = value.trim();
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key.trim();
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value.trim();
  }

  public boolean isKeyEqual(KeyValuePair kvPair){
    return this.key.equals(kvPair.key);
  }

  public boolean isValueEqual(KeyValuePair kvPair){
    return this.value.equals(kvPair.value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyValuePair that = (KeyValuePair) o;
    return Objects.equals(key, that.key) &&
        Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int hashCode = 17;
    hashCode = hashCode*31 + key.hashCode();
    hashCode = hashCode*31 + value.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return this.key+"-"+this.value;
  }
}
