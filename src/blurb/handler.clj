(ns blurb.handler
  (:use compojure.core)
  (:require [compojure.handler :as handler]
            [compojure.route :as route]
            [ring.middleware.json :as json]
            [blurb.zookeeper :as zk]))

(defn clusters
  []
  (let [client (zk/get-client)]
    (zk/get-clusters client)))

(defn tables
  [cluster]
  (let [client (zk/get-client)]
    (zk/get-tables client cluster)
    cluster))

(defn registered-nodes
  [cluster]
  (let [client (zk/get-client)]
    (zk/get-registered-nodes client cluster)))

(defn online-nodes
  [cluster]
  (let [client (zk/get-client)]
    (zk/get-online-nodes client cluster)))

(defn node-version
  [cluster node]
  (let [client (zk/get-client)]
    (zk/get-node-version client cluster node)))

(defroutes app-routes
  (GET "/" [] "Hello World")
  (GET "/clusters" [] (clusters))
  (GET "/clusters/:cluster/tables" [cluster] (tables cluster))
  (GET "/clusters/:cluster/registered-nodes" [cluster] (registered-nodes cluster))
  (GET "/clusters/:cluster/online-nodes" [cluster] (online-nodes cluster))
  (GET "/clusters/:cluster/online-nodes/:node/version" [cluster node] (node-version cluster node))
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (->
    (handler/site app-routes)
    (json/wrap-json-response)))
