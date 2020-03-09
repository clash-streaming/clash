# CLASH



## Code style

[ktlint](https://ktlint.github.io/) is used as a linter and formatter.
It is embedded to gradle with [this plugin](https://github.com/jlleitschuh/ktlint-gradle).

In doubt of what ktlint does, its debug mode can be enabled by adding this ot `build.gradle.kts`:
```
ktlint {
    debug.set(true)
}
```

Even more static code analysis is done by [detekt](https://github.com/arturbosch/detekt).
Configuration for detekt can be found in `config/detekt/detekt.yml`.
