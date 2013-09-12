(defproject blurb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [compojure "1.1.5"]
                 [ring/ring-json "0.2.0"]
                 [org.apache.curator/curator-framework "2.2.0-incubating"]]
  ;0.3.0-incubating-SNAPSHOT
  :plugins [[lein-ring "0.8.5"]]
  :ring {:handler blurb.handler/app}
  :profiles
  {:dev {:dependencies [[ring-mock "0.1.5"]]}})
