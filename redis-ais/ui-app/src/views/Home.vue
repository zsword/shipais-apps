<template>
  <div class="home">
    <ais-redis-clean />
  </div>
</template>

<script>
// @ is an alias to /src
import AisRedisClean from './data/AisRedisClean.vue'

export default {
  name: 'home',
  components: {
    AisRedisClean
  }
}
</script>

<style scoped>
.home {
  position: relative;
}
</style>
