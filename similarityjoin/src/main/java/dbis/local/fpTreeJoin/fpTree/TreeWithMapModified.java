package dbis.local.fpTreeJoin.fpTree;

import dbis.local.fpTreeJoin.utils.KeyValuePair;

import java.io.Serializable;
import java.util.*;

public class TreeWithMapModified implements Serializable {
  private NodeWithMap root;
  public Map<KeyValuePair, List<NodeWithMap>> helperTableAllNodes;
  public static int nodeIds;
  public HashSet<Integer> coveredNodes;
  public HashSet<Integer> branchesToIgnore;
  public int treeDepth;
  public HashMap<Integer, Integer> numberOfChildrenPerLevel;
  public int maxChildren;

  public TreeWithMapModified(){
    this.root = new NodeWithMap("root","root");
    this.root.setParent(null);
    this.helperTableAllNodes = new HashMap<>();
    this.coveredNodes = new HashSet<>();
    this.branchesToIgnore = new HashSet<>();
    nodeIds = 1;
    this.treeDepth = 0;
    this.numberOfChildrenPerLevel = new HashMap<>();
    this.maxChildren = 0;
  }

  public NodeWithMap getRoot(){
    return this.root;
  }

  /**
   * Method for adding the key-value pairs of the document as nodes in the FP-tree.
   * @param keyValuePairs
   * @param documentId
   */
  public void addNewNodesFromDocument(List<KeyValuePair> keyValuePairs, String documentId){
    /**
     * create node objects from the key-value pairs
     */
    ArrayList<NodeWithMap> tmpNodes = new ArrayList<>();
    for(KeyValuePair kvPair : keyValuePairs){
      NodeWithMap node = new NodeWithMap(kvPair);
      tmpNodes.add(node);
    }

    //call the method for finding the position of the nodes in the tree
    addNewNodesFromDocument(tmpNodes, documentId, this.root, 0);
  }

  /**
   * Method for adding a new node in the tree if the node provided as input doesn't exist in the tree.
   * If the node exists in the tree then the children of the node are recursively checked with the input.
   * @param documentNodes
   * @param documentId
   * @param rootNode
   * @param nextKVPairToConsider
   */
  private void addNewNodesFromDocument(ArrayList<NodeWithMap> documentNodes, String documentId, NodeWithMap rootNode, int nextKVPairToConsider){
    //if we have covered all of the nodes provided as input then break
    if(nextKVPairToConsider < documentNodes.size()) {
      //take the next node from the child nodes of the rootNode provided as input
      NodeWithMap rootDocumentNode = documentNodes.get(nextKVPairToConsider);

      //search for the node if it exists in the current branch
      NodeWithMap existingNode = searchForNodeWithSameKvPair(rootDocumentNode,rootNode);
      //if the node doesn't exist in the tree then add the node in the tree and also recursively
      //add all of the children of the node
      if (existingNode == null) {
        //call the method for adding the nodes in the tree
        recursivelyAddNewChildrenToRoot(rootDocumentNode, documentNodes, documentId, rootNode, nextKVPairToConsider);

        //once this method is called all of the nodes have been added as branches to the tree so break
//                return;
      }
      else {//otherwise go to the next node in the list of key-value pairs of the document since
        //the previous node already exists in the branch
        nextKVPairToConsider+=1;
        if(nextKVPairToConsider >= documentNodes.size()){
          //if all of the children nodes have been covered then inform that the document
          //with id documentId has been covered by storing the document in the last child node
          existingNode.addDocumentId(documentId);
        }else {
          //if there are still children left call the same method now with the found node as root
          addNewNodesFromDocument(documentNodes, documentId, existingNode, nextKVPairToConsider);
        }
      }
    }else{
      return;
    }
  }

  /**
   * Method for populating the helper table. The key represents a certain key-value pair and the
   * value a list of all the nodes that are labeled with the key-value pair such that the first node in the list
   * is the first occurrence of the key-value pair.
   * @param node
   */
  public void addNodesInHelperTable(NodeWithMap node){
    List<NodeWithMap> existingNodes = this.helperTableAllNodes.get(node.getKeyValuePair());
    if(existingNodes==null){
      existingNodes = new LinkedList<>();
    }

    existingNodes.add(node);
    this.helperTableAllNodes.put(node.getKeyValuePair(), existingNodes);

  }

  /**
   * Method for adding nodes as children of the 'rootNode' provided as input. It takes as input the first node ('topNode') of the document ('documentId') that is not
   * present in the FP-tree, the list of all key-value pairs of the document ('documentNodes'), the root node ('rootNode') to which the nodes should be appended and the
   * 'currentIndex' representing the position of the 'topNode' in the list 'documentNodes'.
   * @param topNode
   * @param documentNodes
   * @param documentId
   * @param rootNode
   * @param currentIndex
   */
  public void recursivelyAddNewChildrenToRoot(NodeWithMap topNode, ArrayList<NodeWithMap> documentNodes, String documentId, NodeWithMap rootNode, int currentIndex){
    //if the document has more then one node that needs to be added to the branch then add them all to the 'rootNode'
    if(documentNodes.size() > 1 && currentIndex + 1 < documentNodes.size()) {
      //take the last note from the document nodes ordered by their occurrence in the documents
      NodeWithMap lastNode = documentNodes.get(documentNodes.size() - 1);
      //inform that this nodes and all before it until the root come from document with id 'documentId'
      lastNode.addDocumentId(documentId);
      //update the helper table
      addNodesInHelperTable(lastNode);
      //iterate over all of the other kv-pairs starting from the bottom
      for (int i = documentNodes.size() - 2; i > currentIndex; i--) {
        //take the current node
        NodeWithMap currentNode = documentNodes.get(i);
        //add to it as children the last node
        currentNode.addChild(lastNode,nodeIds);
        nodeIds+=1;
        //update the helper table
        addNodesInHelperTable(currentNode);
        //switch the last node with the current node
        lastNode = currentNode;
      }
      //to the top node add the last node as children
      topNode.addChild(lastNode,nodeIds);
      nodeIds+=1;
      //update the helper table
      addNodesInHelperTable(topNode);
      //update the parent node of the top node
      rootNode.addChild(topNode,nodeIds);
      nodeIds+=1;
    }else{//if the document has only one node then add it to the root node provided as input
      //inform that this node comes from document 'documentId'
      topNode.addDocumentId(documentId);
      //update the helper table
      addNodesInHelperTable(topNode);
      //add the node to the root
      rootNode.addChild(topNode,nodeIds);
      nodeIds+=1;
    }
  }

  /**
   * Method for finding if the node provided as input exists in the tree and if so returns the node otherwise
   * returns null
   * @param node
   * @param parentNode
   * @return
   */
  public NodeWithMap searchForNodeWithSameKvPair(NodeWithMap node, NodeWithMap parentNode){
    return parentNode.getChildren().get(node.getKeyValuePair());
  }

  /**
   * Update branches of FPTree such that the parent branch
   * is reflect in the children branch
   * @param rootNode
   */
  public void updateNodesDepth(NodeWithMap rootNode){
    int childNumber=1;
    for(KeyValuePair kvPair : rootNode.getChildren().keySet()){
      NodeWithMap childNode = rootNode.getChildren().get(kvPair);
      childNode.nodeDepth = rootNode.nodeDepth + 1;
      childNode.branchId = childNumber+10*rootNode.branchId;
      updateNodesDepth(childNode);
      childNumber+=1;
    }
  }

  /**
   * Update branches of FPTree such that the parent branch
   * is reflect in the children branch but use Strings in order to represent the branchIds
   * @param rootNode
   */
  public void updateNodesDepthThroughStrings(NodeWithMap rootNode){
    int childNumber=1;
    for(KeyValuePair kvPair : rootNode.getChildren().keySet()){
      NodeWithMap childNode = rootNode.getChildren().get(kvPair);
      childNode.nodeDepth = rootNode.nodeDepth + 1;
      //if we are updating the branchIds for the first children of the root ignore the branchId of the root
      childNode.branchIdAsText = (rootNode.branchIdAsText.equals(""))?String.valueOf(childNumber):childNumber+"_"+rootNode.branchIdAsText;
      updateNodesDepthThroughStrings(childNode);
      childNumber+=1;
    }
  }

  /**
   * Check if we have previously found a parent node on the same branch whose key-value pairs were
   * in conflict with the key-value pairs of the document. Returns true if the node should be ignored and
   * returns false otherwise
   * @param branchesToIgnore
   * @param branchId
   * @return
   */
  public boolean ignoreNodeBasedOnBranchId(HashSet<String> branchesToIgnore, String branchId){
    //if we haven't added any branch for ignoring yet just stop the search immediately
    if(branchesToIgnore.size()==0){
      return false;
    }

    StringBuilder tmpBranchId = new StringBuilder(branchId);
    String branchIdToCheck = "";
    boolean ignoreBranch = false;
    while (true){
      int charIndex = tmpBranchId.lastIndexOf("_");
      if(charIndex!=-1){
        //get the id of the branch that needs to be checked
        branchIdToCheck = tmpBranchId.substring((charIndex+1),tmpBranchId.length()) + (branchIdToCheck.equals("")?"":"_") + branchIdToCheck;
        //replace the branchId by removing the part that is being already checked
        tmpBranchId = new StringBuilder(tmpBranchId.substring(0, charIndex));
        //if the branch has been already covered then break
        if(branchesToIgnore.contains(branchIdToCheck)){
          ignoreBranch = true;
          break;
        }
      }else{
        if(branchIdToCheck.equals("")){
          //if branchIdToCheck is empty it means that the branchId is made up of only one integer
          branchIdToCheck = tmpBranchId.toString();
        }else{
          //this means that we have already went through several children and there is only one more left to append
          branchIdToCheck = tmpBranchId+"_"+branchIdToCheck;
        }
        ignoreBranch = branchesToIgnore.contains(branchIdToCheck)?true : false;
        break;
      }

    }
    return ignoreBranch;
  }

  /**
   * Method for obtaining the branchId of the top node (first child of root) out
   * of the branch id of the node provided as input.
   * @param nodeBranchId
   * @return
   */
  public String getTopNodeBranchId(String nodeBranchId){
    int charIndex = nodeBranchId.lastIndexOf("_");
    return nodeBranchId.substring((charIndex+1));
  }

  /**
   * Navigate directly through the children of the root such that the levels of the FP-tree that are represented by a key present in all of the documents
   * will be immediately traversed by selecting the equally labeled node as some key-value pair in the investigated document.
   * @param documentId
   * @param kvPairs
   * @param keys
   * @param joinableDocuments
   * @param branchesToIgnoreHelper
   * @param numOfKeysInEveryDoct
   * @return
   */
  public Set<String> searchingForJoinableDocumentsByUsingKVPairsOfDocument(String documentId, List<KeyValuePair> kvPairs, ArrayList<String> keys,HashSet<String> joinableDocuments, HashSet<String> branchesToIgnoreHelper, int numOfKeysInEveryDoct){
    NodeWithMap childOfRoot = null;
    NodeWithMap nextNodeToConsider = this.root;
    //skip trough the levels of the FP-tree that have ubiquitous keys
    for(int i=0;i<numOfKeysInEveryDoct;i++){
      childOfRoot = nextNodeToConsider.getChildren().get(kvPairs.get(i));
      //gather all the documents represented by the 'childOfRoot' node, if there are such documents
      joinableDocuments.addAll(childOfRoot.getDocumentIds());
      //make the child node the root node in the next iteration
      nextNodeToConsider = childOfRoot;
    }

    //it means that we have skipped some levels of the FP-tree
    if(childOfRoot!=null){
      boolean containsKvPair = kvPairs.contains(childOfRoot.getKeyValuePair());

      //in order to know whether a join is possible
      //informing that at least one join partner was found
      //and also store documents for the current node if there are some
      HashSet<KeyValuePair> sharedKVPairs = new HashSet<>();
      if(containsKvPair){
        sharedKVPairs.add(childOfRoot.getKeyValuePair());
      }
      //find the joinable documents by traversing the key-value pairs
      joinableDocuments.addAll(childOfRoot.performSearchSecondApproach(new HashSet<>(kvPairs),new HashSet<>(keys),joinableDocuments,childOfRoot, sharedKVPairs, 0,documentId));
      //make sure that the 'documentId' won't be part of the list of joinable documents
      joinableDocuments.remove(documentId);
    }else{
      //it means that there is no key that is present in all of the documents
      joinableDocuments.addAll(searchForJoinableThroughChildren(documentId,new HashSet<>(kvPairs),new HashSet<>(keys),joinableDocuments,0));
    }

    return joinableDocuments;
  }

  /**
   * Search for joinable documents by iterating through the Helper Table which contains pointers of all of the nodes with equal
   * key-value pairs
   * @param documentId
   * @param kvPairs
   * @param keys
   * @param joinableDocuments
   * @return
   */
  public Set<String> searchForJoinableDocumentsThread(String documentId, List<KeyValuePair> kvPairs, ArrayList<String> keys,HashSet<String> joinableDocuments, HashSet<String> branchesToIgnoreHelper){
    //iterate over all of the key-value pairs of the document provided as input to the method
    for(int i=0;i<kvPairs.size();i++){

      KeyValuePair kvPair = kvPairs.get(i);
      //find all of the linked nodes from the helper table
      List<NodeWithMap> linkedNodes = this.helperTableAllNodes.get(kvPair);
      //iterate over all of the link nodes
      for(int j=0;j<linkedNodes.size();j++){
        NodeWithMap coveredLinkedNode = linkedNodes.get(j);

        //check if the linked node is already located on a branch that should be ignored
        //or if the node has already been covered through the previous iterations
        boolean performCheck = !ignoreNodeBasedOnBranchId(branchesToIgnoreHelper,coveredLinkedNode.branchIdAsText);

        if(performCheck){
          //check if the parents of the nodes are IN CONFLICT with the key-value pairs of the document
          boolean goIn = coveredLinkedNode.performSearchIfCandidate(kvPairs,keys,coveredLinkedNode,branchesToIgnoreHelper);
          if (goIn) {
            //if the parents are not in conflict, which means that the branch represented by the node coveredLinkedNode is a candidate branch
            //go through all of the children and find which documents are joinable
            joinableDocuments.addAll(coveredLinkedNode.performSearch(new HashSet<>(kvPairs), new HashSet<>(keys),
                joinableDocuments, coveredLinkedNode, branchesToIgnoreHelper));
          }
        }
      }

    }
    //just make sure that the 'documentId' won't be part of the joinableDocuments
    joinableDocuments.remove(documentId);

    return joinableDocuments;
  }

  /**
   * For given document find all of the joinable documents by iterating through the children of the root
   * @param documentId
   * @param kvPairs
   * @param keys
   * @param joinableDocuments
   * @param numOfDocuments
   * @return
   */
  public Set<String> searchForJoinableThroughChildren(String documentId, HashSet<KeyValuePair> kvPairs, HashSet<String> keys, HashSet<String> joinableDocuments, int numOfDocuments){
    for(KeyValuePair kvPair : this.root.getChildren().keySet()){
      NodeWithMap childOfRoot = this.root.getChildren().get(kvPair);

      boolean containsKey = keys.contains(childOfRoot.getKeyValuePair().getKey());
      boolean containsKvPair = kvPairs.contains(childOfRoot.getKeyValuePair());
      //if the root's child is in conflict then skip the branch that it represents
      if(containsKey && !containsKvPair){
        continue;
      }

      //in order to know whether a join is possible
      //informing that at least one join partner that was found
      HashSet<KeyValuePair> sharedKVPairs = new HashSet<>();
      if(containsKvPair){
        sharedKVPairs.add(childOfRoot.getKeyValuePair());
      }
      //go through the children and search for joinable documents
      joinableDocuments.addAll(childOfRoot.performSearchSecondApproach(kvPairs,keys,joinableDocuments,childOfRoot, sharedKVPairs, numOfDocuments,documentId));

    }
    //to be sure remove the 'documentId' if it is present in the list of joinable documents
    joinableDocuments.remove(documentId);
    return joinableDocuments;
  }

  public void printTree(){
    this.root.printNode();
  }

  /**
   * getting the biggest depth of the tree
   * @param node
   */
  public void getDepthOfNodes(NodeWithMap node){
    for(KeyValuePair kvPair : node.getChildren().keySet()){
      NodeWithMap childNode = node.getChildren().get(kvPair);
      if(childNode.nodeDepth > this.treeDepth){
        this.treeDepth = childNode.nodeDepth;
      }

      getDepthOfNodes(childNode);
    }
  }

  /**
   * calculating the number of children per particular level
   * @param node
   */
  public void calculateChildrenPerLevel(NodeWithMap node){
    for(KeyValuePair kvPair : node.getChildren().keySet()){
      NodeWithMap childNode = node.getChildren().get(kvPair);

      Integer previousNumber = this.numberOfChildrenPerLevel.get(childNode.nodeDepth);
      if(previousNumber==null){
        previousNumber = 0;
      }
      previousNumber++;

      this.numberOfChildrenPerLevel.put(childNode.nodeDepth,previousNumber);

      calculateChildrenPerLevel(childNode);
    }
  }

  public void calculateChildrenPerNode(NodeWithMap node){
    for(KeyValuePair kvPair : node.getChildren().keySet()){
      NodeWithMap childNode = node.getChildren().get(kvPair);

      if(childNode.getChildren().size() > this.maxChildren){
        this.maxChildren = childNode.getChildren().size();
      }

      calculateChildrenPerLevel(childNode);
    }
  }

}
