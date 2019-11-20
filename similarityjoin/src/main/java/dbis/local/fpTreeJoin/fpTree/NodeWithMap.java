package dbis.local.fpTreeJoin.fpTree;

import dbis.local.fpTreeJoin.utils.KeyValuePair;

import java.io.Serializable;
import java.util.*;

public class NodeWithMap implements Serializable {
  private NodeWithMap parent;
  private Map<KeyValuePair, NodeWithMap> children;
  private KeyValuePair keyValuePair;
  private Set<String> documentIds;
  public int nodeDepth;
  public int nodeId;
  public int branchId;
  public List<Integer> parentIds;
  /**
   * variable that will represent the id of the branch on which the node is located.
   * But now instead of using an integer we will use a string where with "_" we will
   * separate the branch ids from the previous level
   * Example. for the tree a:2->b:3->c:4 we will have the branch ids
   * a:2 "1"
   * b:3 "1_1"
   * c:4 "1_1_1"
   */
  public String branchIdAsText;

  public NodeWithMap(String key, String value){
    this.keyValuePair = new KeyValuePair(key,value);
    this.children = new HashMap<>();
    this.documentIds = new HashSet<>();
    this.parentIds = new ArrayList<>();
    branchIdAsText = "";
  }

  public NodeWithMap(KeyValuePair kvPair){
    this.keyValuePair = kvPair;
    this.children = new HashMap<>();
    this.documentIds = new HashSet<>();
    this.parentIds = new ArrayList<>();
    branchIdAsText = "";
  }

  public void setParent(NodeWithMap node){
    this.parent = node;
  }

  public NodeWithMap getParent(){
    return this.parent;
  }

  public KeyValuePair getKeyValuePair(){
    return this.keyValuePair;
  }

  public void addDocumentId(String documentId){
    this.documentIds.add(documentId);
  }

  public Set<String> getDocumentIds(){
    return this.documentIds;
  }

  public Map<KeyValuePair, NodeWithMap> getChildren(){return this.children;}

  public void addChild(NodeWithMap childNode, int nodeId){
    childNode.setParent(this);
    childNode.nodeId = nodeId;

    this.children.put(childNode.getKeyValuePair(), childNode);
  }

  /**
   * Method for checking if the parents of the node provided as input are in conflict with the key-value pairs of the document
   * for which the check for joinable documents is performed
   * @param keyValuePairs
   * @param keys
   * @param node
   * @param branchesToIgnore
   * @return
   */
  public boolean performSearchIfCandidate(List<KeyValuePair> keyValuePairs, List<String> keys, NodeWithMap node, HashSet<String> branchesToIgnore){
    //if the node has no more parents then return that the node is NOT in conflict
    if(node.parent == null || node.parent.getKeyValuePair().getKey().equals("root")){
      return true;
    }

    KeyValuePair parentKvPair = node.parent.keyValuePair;
    //check if the parent node has a key which is present in the keys of the document but with different value
    //meaning that the key-value pair of the parent node is in conflict with the key-value pairs of the document
    if(keys.contains(parentKvPair.getKey()) && !keyValuePairs.contains(parentKvPair)){
      //store the branchId of the node in conflict so that pruning can be performed
      branchesToIgnore.add(node.parent.branchIdAsText);
      return false;
    }else{
      //if the parent is not in conflict perform the same check for his parent
      return performSearchIfCandidate(keyValuePairs, keys, node.parent, branchesToIgnore);
    }
  }

  public boolean performSearchIfCandidateAndIfNodeShouldBeIgnored(List<KeyValuePair> keyValuePairs, List<String> keys, NodeWithMap node, HashSet<Integer> branchesToIgnore){
    //if the node has no more parents then return that the node is NOT in conflict
    if(node.parent == null || node.parent.getKeyValuePair().getKey().equals("root")){
      return true;
    }

    KeyValuePair parentKvPair = node.parent.keyValuePair;
    //check if the node (parent) is located on a branch that should be ignored
    boolean check1 = branchesToIgnore.contains(node.branchId);
    //check if the parent of the node is in conflict with the key-value pairs of the document
    boolean check2 = (keys.contains(parentKvPair.getKey()) && !keyValuePairs.contains(parentKvPair));
    if(check1 || check2 ){
      if(check2){//if the parent is in conflict then add its branch id to the branchesToIgnore
        branchesToIgnore.add(node.parent.branchId);
      }
      return false;
    }else{
      //if the parent is not in conflict perform the same check for his parent and the node is not located on a branch that should be ignored
      return performSearchIfCandidateAndIfNodeShouldBeIgnored(keyValuePairs, keys, node.parent, branchesToIgnore);
    }
  }

  /**
   * Method that finds joinable documents for the document provided as input by checking the child nodes of the
   * node provided as input
   * @param keyValuePairs
   * @param keys
   * @param joinableDocuments
   * @param node
   * @param branchesToIgnore
   * @return
   */
  public HashSet<String> performSearch(Set<KeyValuePair> keyValuePairs, Set<String> keys, HashSet<String> joinableDocuments, NodeWithMap node, HashSet<String> branchesToIgnore){
    //inform that all the documents for 'node' are join partners for the current document
    joinableDocuments.addAll(node.documentIds);

    //if 'node' has no more children then it means that we have came to a leaf and
    //the method returns all the join partners that were found
    if(node.getChildren().size()==0){
      branchesToIgnore.add(node.branchIdAsText);
      return joinableDocuments;
    }

    //iterate through all of the children of the node
    for(KeyValuePair kvPair : node.getChildren().keySet()){
      NodeWithMap childNode = node.getChildren().get(kvPair);
      //inform that the node has been already covered
      branchesToIgnore.add(childNode.branchIdAsText);
      //if the child is not in conflict with the key-value pairs of the document then gather all of the documentIds in the node and continue with the children
      if(!(keys.contains(childNode.getKeyValuePair().getKey()) && !keyValuePairs.contains(childNode.getKeyValuePair()))){
        joinableDocuments.addAll(performSearch(keyValuePairs,keys,joinableDocuments,childNode, branchesToIgnore));
      }
    }
    return joinableDocuments;
  }

  /**
   * The method needs to make multiple checks because it can happen that the keys that are immediate children of the root are not present in
   * all of the documents and as a result there will not be a conflict with some branches meaning that as we go down through the leafs we need
   * to make sure that the documents can be joined by at least one key-value pair otherwise we cannot gather all the joinable documents.
   * @param keyValuePairs
   * @param keys
   * @param joinableDocuments
   * @param node
   * @param sharedKVPairs
   * @param numOfDocuments
   * @param documentId
   * @return
   */
  public HashSet<String> performSearchSecondApproach(HashSet<KeyValuePair> keyValuePairs, HashSet<String> keys, HashSet<String> joinableDocuments, NodeWithMap node, HashSet<KeyValuePair> sharedKVPairs, int numOfDocuments, String documentId){
    //if there is at least one shared key-value pair with the document then collect all the joinable documents
    if(sharedKVPairs.size() > 0){
      joinableDocuments.addAll(node.documentIds);
    }

    //if a leaf node has been reached return the joinable documents
    if(node.getChildren().size()==0){
      return joinableDocuments;
    }
    //iterate over the key-value pairs of the node
    for(KeyValuePair kvPair : node.getChildren().keySet()){
      //create a list storing the temporary shared key-value pairs of
      //the node with the document
      HashSet<KeyValuePair> tmpAddedKvPairs = new HashSet<>();
      NodeWithMap childNode = node.getChildren().get(kvPair);
      boolean containsKey = keys.contains(childNode.getKeyValuePair().getKey());
      boolean containsKvPair = keyValuePairs.contains(childNode.getKeyValuePair());

      if(containsKey && !containsKvPair){
        continue;
      }else{
        //inform that the current examined branch
        //shares key-value pairs with the document
        if(containsKvPair){
          sharedKVPairs.add(childNode.getKeyValuePair());
          //keep a temporary array of all the nodes that were added during
          //the top-down navigation through the branches
          tmpAddedKvPairs.add(childNode.getKeyValuePair());
        }

        joinableDocuments.addAll(performSearchSecondApproach(keyValuePairs,keys,joinableDocuments,childNode, sharedKVPairs, numOfDocuments,documentId));
        //once a child node has been covered remove all of the key-value pairs
        //for that child node form the list of shared key-value pairs
        sharedKVPairs.removeAll(tmpAddedKvPairs);
      }

    }
    return joinableDocuments;
  }

  public Set<String> performSearchAsCombinationOfBothApproaches(HashSet<KeyValuePair> keyValuePairs, HashSet<String> keys, HashSet<String> joinableDocuments, NodeWithMap node, HashSet<KeyValuePair> sharedKVPairs, HashSet<String> branchesToIgnoreHelper){

    return joinableDocuments;
  }

  /**
   * Method for printing the node together with its children nodes.
   */
  public void printNode(){
    String documentInfo = documentIds.size() > 0 ? documentIds.toString() : "";
    System.out.print("Node: "+nodeId+" Branch: " + branchIdAsText+" [Key: " + this.keyValuePair.getKey() + ", Val: " + this.keyValuePair.getValue() + ", Strenght: " + this.keyValuePair.numOfDocsForKey + "] - " + documentInfo);
    System.out.print("\n");

    for(KeyValuePair kvPair : this.children.keySet()){
      NodeWithMap child = this.children.get(kvPair);
      for (int i = 0; i < child.nodeDepth; i++) {
        System.out.print("\t");
      }
      child.printNode();
    }

    return;

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeWithMap node = (NodeWithMap) o;
    return Objects.equals(nodeId,node.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parent, children, keyValuePair, documentIds);
  }

}
