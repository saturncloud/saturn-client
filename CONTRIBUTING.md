## Releases

Push a tag on `main` or `release-*` branch:

```
git tag -a 0.0.3 -m "Release 0.0.3"
git push --tags
```

Any tag that you push will kick off a GitHub Action which automatically publishes the package to PyPi.
