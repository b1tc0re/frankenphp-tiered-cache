# Changelog

## [0.1.0](https://github.com/b1tc0re/frankenphp-tiered-cache/compare/v0.0.1...v0.1.0) (2026-09-19)


### Features

* **cache:** add atomic counter operations ([3338ea2](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/3338ea209008832a7aba78a098e5709b4321f474))
* **cache:** add atomic TieredCache Add ([8d64880](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/8d6488006f0ca3137faca30b25de18f9cc56de3f))
* **cache:** add batch reads for TieredCache ([762ec1b](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/762ec1bd29dc762fbb0352d120fd3e5859fef548))
* **cache:** add transactional SetMany support ([286f5c3](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/286f5c3b4b51c33ec819f1946d4d484cf122e733))
* **cache:** add TTL-aware tiered composition ([22f4a1b](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/22f4a1bae3e71c13f4f22840de8a44908ce0abde))
* **extension:** embed release version ([ecdcc4e](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/ecdcc4e2840f672b32139ab29cbf84ca5d083ee4))
* **extension:** prepare first versioned release ([1b02a0d](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/1b02a0d489a7143051757a51791ec8fa85635f55))
* **extension:** wire native API to TieredCache ([e8be41a](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/e8be41ad6e2ded210b85836141794b0c516a77e8))
* **observability:** aggregate memory pressure logs ([843f90c](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/843f90ca04f45850d503d1c1eb4cd31a67b51028))
* **redis:** add synchronous Redis cache backend ([f0e030d](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/f0e030da07b96e621d5edf72b7e6df440705a0dd))
* **tiered:** add Redis cross-pod invalidation ([e57510c](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/e57510c36f38bfdded4c6db93b5b341b2c949014))
* **tiered:** clean dirty L2 keys during recovery ([f256edf](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/f256edfbed76933e24b58b4d9437dbc4917a0abc))
* **tiered:** fail closed when L2 becomes unavailable ([834d9f0](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/834d9f0d6cf22b738b2966e98c336a6f0b6ba912))


### Bug Fixes

* **cache:** keep Redis command errors out of degraded state ([6b7c691](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/6b7c691d79b58f0418ce35f18f0ac08312f88b4e))
* **observer:** aggregate eviction notifications ([c2b0e8d](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/c2b0e8debfae362019dfa9d61726fe2b9b5849d5))
* **redis:** clean completed fence entries ([68fbf1d](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/68fbf1d200aab3e526a9888371d1c497cdf86504))
* **redis:** expire abandoned fence reservations ([66a44c9](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/66a44c97c6b11cd85991600762bcbcc54cd862e2))
* **redis:** fail safe on unknown counter server errors ([5776ddb](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/5776ddbf0af34270f797b9ee989a411edd42f5f3))
* **redis:** handle DECRBY minimum overflow ([8ae580f](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/8ae580f324389d06611277ffeb76de5f3dfc1acd))
* **redis:** separate command and infrastructure errors ([7c554c7](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/7c554c7cb0428fe42ab867482f395bff6d7ee0a9))
* **tiered:** account for L2 latency in local TTL ([512762c](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/512762c6c1ab7ab33f1b1e2b5c8e0d82caa5fb0c))
* **tiered:** cancel stale async writes after invalidation ([c8e7724](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/c8e772470d2b86583dace3fb09044a579f969047))
* **tiered:** close degradation race before waking readers ([fab414e](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/fab414ecc7f1d1e5e8c8a01e6464f6dc96da24d8))
* **tiered:** delete dirty L2 keys before invalidation ([cb19aec](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/cb19aec3779ddc09e5d319b1f800661438dbca31))
* **tiered:** fence asynchronous Redis writes ([b577046](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/b577046160fd7124fb4078e45b6b624e73cbe141))
* **tiered:** fence dirty-key recovery ([2f37d6b](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/2f37d6b851b621f00a9ad9bacfdde1bd0c9ae81a))
* **tiered:** generate process-independent invalidation origin ([2c46f92](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/2c46f92eda829ffccb9f8317c3fafacbde636d1b))
* **tiered:** preserve committed counter results ([e98b01e](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/e98b01ea524390911956ba68222de85500c31eb5))
* **tiered:** preserve TTL across async L2 writes ([545212d](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/545212dee1868087805a54881dbcaebb97a0c1f5))
* **tiered:** protect Get from closed backends ([059fce3](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/059fce3e1c8e8363f9de8675018dcc501ab8c12a))
* **tiered:** release fences after failed L1 mutations ([a0da557](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/a0da557ff87fb2bad35bb79e8e543474d96388f9))
* **tiered:** serialize healthy state cleanup ([6b2be65](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/6b2be659682715067388ad2e242b62b9eaeffc7c))
* **tiered:** wait for pending L2 writes on L1 miss ([cc3d4f7](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/cc3d4f73e9beeddbe01cc1954fadb576eb1af555))


### Reverts

* expire abandoned fence reservations ([95945e9](https://github.com/b1tc0re/frankenphp-tiered-cache/commit/95945e9537cbfa275b6677dee0b0f7b4ee2ffd35))

## Changelog
