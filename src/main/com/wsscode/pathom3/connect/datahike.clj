(ns com.wsscode.pathom3.connect.datahike
  (:require
   [datahike.api :as d]))

(defn wrap-ident [value]
  {:db/ident value})

(def datahike-config
  {:com.wsscode.pathom3.connect.datomic/datomic-driver-q  d/q
   :com.wsscode.pathom3.connect.datomic/datomic-driver-db d/db
   :com.wsscode.pathom3.connect.datomic/get-db-schema
   (fn [{:keys [schema]}]
     (into {}
           (comp
            (remove (fn [[attr data]]
                      (or (number? attr)
                          (re-find #"^db\.?" (namespace attr)))))
            (map (fn [[attr data]]
                   (clojure.lang.MapEntry.
                    attr
                    (update data :db/valueType wrap-ident)))))
           schema))})
