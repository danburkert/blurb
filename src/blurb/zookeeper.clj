(ns blurb.zookeeper
  (:require [zookeeper :as zk]))

(defn get-client []
  (zk/connect "localhost:2181"))

(defn get-clusters
  "Takes a zookeeper client and returns a sequence of cluster names"
  [client]
  (zk/children client "/blur/clusters"))

(defn get-tables
  "Takes a zookeeper client and a cluster name, and returns a sequence of the
   tables in the cluster"
  [client cluster]
  (zk/children client (str "/blur/clusters/" cluster "/tables")))

(defn get-registered-nodes
  "Takes a zookeeper client and a cluster name, and returns a sequence of the
   registered nodes in the cluster"
  [client cluster]
  (zk/children client (str "/blur/clusters/" cluster "/registered-nodes")))

(defn get-online-nodes
  "Takes a zookeeper client and a cluster name, and returns a sequence of the
   online nodes in the cluster"
  [client cluster]
  (zk/children client (str "/blur/clusters/" cluster "/online-nodes")))

(comment

  (def zk
    (zk/connect "localhost:2181"))

  (identity zk)
  (zk/children zk "/blur")
  (zk/children zk "/blur/clusters")
  (zk/children zk "/blur/clusters/default")
  (zk/children zk "/blur/clusters/default/online-nodes")
  (zk/children zk "/blur/clusters/default/registered-nodes")
  (zk/children zk "/blur/clusters/default/tables")

  (zk/data zk "/blur/clusters/default/safemode")
  (zk/children zk  "/blur/clusters/default")

  (get-clusters zk)
  (get-tables zk "default")
  (type (get-registered-nodes zk "default"))
  (get-online-nodes zk "default")

 )
