# Library Build Configuration

This project is now configured as a Clojure library using **tools.build**.

## Library Information

- **Coordinates**: `io.github.fgasperino/csp-clj`
- **Version**: `0.0.0`
- **License**: EPL-2.0 (Eclipse Public License v2.0)
- **Developer**: Franco Gasperino <franco.gasperino@gmail.com>
- **Repository**: https://github.com/fgasperino/csp-clj

## Build Commands

All build tasks are run via the `:build` alias:

```bash
# Display library information
clj -T:build info

# Build the JAR file (creates target/csp-clj-0.0.0.jar)
clj -T:build jar

# Install to local Maven repository (~/.m2)
clj -T:build install

# Deploy to Clojars (requires credentials)
clj -T:build deploy

# Clean build artifacts
clj -T:build clean
```

## Deploying to Clojars

### Step 1: Create Clojars Account

1. Go to https://clojars.org
2. Click "Register" and create an account with username `fgasperino`
3. Verify your email address

### Step 2: Generate Deploy Token

1. Go to https://clojars.org/tokens
2. Click "Create Token"
3. Give it a name like "csp-clj deploy"
4. Copy the generated token (it starts with `clojars_`)

### Step 3: Deploy

```bash
# Set environment variables
export CLOJARS_USERNAME=your-username
export CLOJARS_PASSWORD=your-deploy-token

# Deploy the library
clj -T:build deploy
```

After deployment, your library will be available at:
https://clojars.org/io.github.fgasperino/csp-clj

Users can add it to their `deps.edn`:

```clojure
{:deps {io.github.fgasperino/csp-clj {:mvn/version "0.0.0"}}}
```

## Files Changed/Created

- **`deps.edn`** - Added `:lib`, `:version`, and `:build` alias
- **`build.clj`** - Build script with jar, install, deploy tasks
- **`.gitignore`** - Added target/, *.jar, pom.xml

## Version Management

To release a new version:

1. Update the version in `build.clj` (line 5)
2. Update the version in `deps.edn` (line 2) - optional but recommended
3. Commit your changes
4. Build and deploy:
   ```bash
   clj -T:build deploy
   ```

## Testing Local Installation

After running `clj -T:build install`, you can test the library in another project:

```clojure
;; In another project's deps.edn
{:deps {org.clojars.fgasperino/csp-clj {:mvn/version "0.0.0"}}}
```

The dependency will resolve from your local `~/.m2` repository.
