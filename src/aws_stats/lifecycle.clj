(ns aws-stats.lifecycle
  "Lifecycle management protocol, used to coordinate startup/shutdown
  of all components in the system.")

(defprotocol Lifecycle
  (start [this]
    "Begin operation of a component. Returns a promise on which the
  caller can block to wait until the component is started.")
  (stop [this]
    "Cease operation, shutting down gracefully where possible. Returns a
  promise on which the caller can block to wait until the component is
  stopped."))

(def empty-promise (doto (promise) (deliver nil)))
