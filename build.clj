(ns build
  "Build script for csp-clj library using tools.build."
  (:require [clojure.tools.build.api :as b]))

(def lib 'org.clojars.fgasperino/csp-clj)
(def version "0.0.0")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean
  "Remove all build artifacts and target directory."
  [_]
  (println "Cleaning target directory...")
  (b/delete {:path "target"})
  (println "Clean complete."))

(defn jar
  "Build a JAR file suitable for library distribution.
   
   This creates:
   - A JAR with compiled classes and source files
   - A generated pom.xml with proper metadata
   
   The JAR will be written to target/<lib>-<version>.jar"
  [_]
  (println "Building JAR for" lib "version" version)

  ;; Clean and create class directory
  (b/delete {:path "target"})
  (.mkdirs (java.io.File. class-dir))

  ;; Copy source files to class directory
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})

  ;; Generate pom.xml with library metadata
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]
                :resource-dirs []
                :scm {:url "https://github.com/fgasperino/csp-clj"
                      :connection "scm:git:git://github.com/fgasperino/csp-clj.git"
                      :developerConnection "scm:git:ssh://git@github.com/fgasperino/csp-clj.git"}
                :pom-data [[:description "Communicating Sequential Processes for Clojure on JDK 24+ Virtual Threads"]
                           [:url "https://github.com/fgasperino/csp-clj"]
                           [:licenses
                            [:license
                             [:name "Eclipse Public License - v 2.0"]
                             [:url "https://www.eclipse.org/legal/epl-2.0/"]
                             [:distribution "repo"]]]
                           [:developers
                            [:developer
                             [:name "Franco Gasperino"]
                             [:email "franco.gasperino@gmail.com"]]]]})

  ;; Build the JAR
  (b/jar {:class-dir class-dir
          :jar-file jar-file
          :basis basis})

  (println "JAR built successfully:" jar-file))

(defn install
  "Install the JAR to the local Maven repository (~/.m2).
   
   This makes the library available for other local projects to depend on."
  [_]
  ;; Build the JAR first if it doesn't exist
  (when-not (.exists (java.io.File. jar-file))
    (println "JAR not found. Building first...")
    (jar nil))

  (println "Installing to local Maven repository...")
  (b/install {:basis basis
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir})
  (println "Install complete. Library available as:")
  (println (str "  " lib " {:mvn/version \"" version "\"}")))

(defn deploy
  "Deploy the JAR to Clojars.
   
   Requires environment variables:
   - CLOJARS_USERNAME: Your Clojars username (your-username)
   - CLOJARS_PASSWORD: Your Clojars deploy token
   
   To generate a deploy token:
   1. Go to https://clojars.org/tokens
   2. Create a new token
   3. Export it as CLOJARS_PASSWORD
   
   Example:
     export CLOJARS_USERNAME=your-username
     export CLOJARS_PASSWORD=your-deploy-token
     clj -T:build deploy"
  [_]
  ;; Build the JAR first if it doesn't exist
  (when-not (.exists (java.io.File. jar-file))
    (println "JAR not found. Building first...")
    (jar nil))

  ;; Check for required environment variables
  (let [username (System/getenv "CLOJARS_USERNAME")
        password (System/getenv "CLOJARS_PASSWORD")]
    (when-not username
      (println "ERROR: CLOJARS_USERNAME environment variable not set")
      (println "  export CLOJARS_USERNAME=your-username")
      (System/exit 1))
    (when-not password
      (println "ERROR: CLOJARS_PASSWORD environment variable not set")
      (println "  Get your deploy token from https://clojars.org/tokens")
      (println "  export CLOJARS_PASSWORD=your-deploy-token")
      (System/exit 1)))

  (println "Deploying to Clojars...")
  (try
    ((requiring-resolve 'deps-deploy.deps-deploy/deploy)
     {:installer :remote
      :artifact jar-file
      :pom-file (str class-dir "/META-INF/maven/" (namespace lib) "/" (name lib) "/pom.xml")
      :sign-releases? false})
    (println "Deploy complete!")
    (println "Library now available at:")
    (println (str "  https://clojars.org/" (namespace lib) "/" (name lib)))
    (catch Exception e
      (println "Deploy failed:" (.getMessage e))
      (throw e))))

(defn info
  "Display library information and build configuration."
  [_]
  (println "Library Information")
  (println "==================")
  (println (str "Library:   " lib))
  (println (str "Version:   " version))
  (println (str "JAR file:  " jar-file))
  (println (str "SCM URL:   https://github.com/fgasperino/csp-clj"))
  (println (str "License:   EPL-2.0"))
  (println "")
  (println "Available tasks:")
  (println "  clean   - Remove build artifacts")
  (println "  jar     - Build library JAR")
  (println "  install - Install to local Maven repository")
  (println "  deploy  - Deploy to Clojars")
  (println "  info    - Display this information"))
