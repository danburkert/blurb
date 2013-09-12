(ns blurb.handler
  (:use compojure.core)
  (:require [compojure.handler :as handler]
            [compojure.route :as route]
            [ring.middleware.json :as json]
            [blurb.zookeeper :as zk]))

(defn clusters
  [ctx]
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
    (str (zk/get-registered-nodes client cluster))))

(defn online-nodes
  [cluster]
  (let [client (zk/get-client)]
    (str (zk/get-online-nodes client cluster))))

(defroutes app-routes
  (GET "/" [] "Hello World")
  (GET "/clusters" [] clusters)
  (GET "/clusters/:cluster/tables" [cluster] tables)
  (GET "/clusters/:cluster/registered-nodes" [cluster] registered-nodes)
  (GET "/clusters/:cluster/online-nodes" [cluster] online-nodes)
  (route/resources "/")
  (route/not-found "Not Found"))

(def app
  (->
    (handler/site app-routes)
    (json/wrap-json-response)))
