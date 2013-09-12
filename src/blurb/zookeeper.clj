(ns blurb.zookeeper
  (:require [clojure.string :as s])
  (:import [org.apache.curator.framework CuratorFrameworkFactory CuratorFramework]
           [org.apache.curator.retry ExponentialBackoffRetry]))

(defprotocol Startable
  (start! [this] "Start necessary long-lived state"))
(defprotocol Stopable
  (stop! [this] "Stop any held long-lived state"))
(defprotocol Refresh
  (refresh! [this] "Reset any held long-lived state"))

(defprotocol Zookeeper
  (clusters [this] "Returns the sequence of cluster names")
  (tables [this cluster] "Returns the sequence of tables names in the cluster")
  (registered-nodes [this cluster] "Returns the sequence of the registered nodes in the cluster")
  (online-nodes [this cluster] "Returns the sequence of the online nodes in the cluster")
  (node-version [this cluster node] "Retrieves the version of the node"))

(defrecord Curator
  [framework]
  Startable
  (start! [this] (.start framework))
  Stopable
  (stop! [this] (.close framework))
  Zookeeper
  (clusters [this]
    (-> framework (.getChildren) (.forPath "/clusters")))
  (tables [this cluster]
    (-> framework (.getChildren) (.forPath (str "/clusters/" cluster "/tables"))))
  (registered-nodes [this cluster]
    (-> framework (.getChildren) (.forPath (str "/clusters/" cluster "/registered-nodes"))))
  (online-nodes [this cluster]
    (-> framework (.getChildren) (.forPath (str "/clusters/" cluster "/online-nodes"))))
  (node-version [this cluster node]
    (-> framework (.getData) (.forPath (str "/clusters/" cluster "/online-nodes/" node)) (String.))))

(defn zookeeper
  "Creates a Curator record, which implements the Zookeeper, Startable, Stopable,
   and Resetable protocols"
  [{:keys [hosts namespace session-timeout connection-timeout] :or {namespace "blur"}}]
  (let [retry (ExponentialBackoffRetry. 1000 3)
        framework (-> (CuratorFrameworkFactory/builder)
                      (.retryPolicy retry)
                      (.connectString (s/join ";" hosts))
                      (.namespace namespace)
                      (cond->
                        session-timeout (.sessionTimeout session-timeout)
                        connection-timeout (.connectionTimeout connection-timeout))
                      (.build))]
    (->Curator framework)))

(comment

  (def zk
    (zookeeper {:hosts ["localhost:2181"]
                :namespace "blur"}))

  (start! zk)
  (clusters zk)
  (tables zk "default")
  (registered-nodes zk "default")
  (online-nodes zk "default")
  (node-version zk "default" "nic-dburkert:40020")
  (stop! zk)

 )
