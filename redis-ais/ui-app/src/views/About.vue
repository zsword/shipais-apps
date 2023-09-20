<template>
  <div class="about">
    <h1>This is an about page</h1>
    <h3>{{info}}</h3>
  </div>
</template>

<script>
import {sayHello} from '../api/hello'

export default {
  setup() {
    console.log('setup')
  },
  data() {
    return {
      info: {
      }
    }
  },
  mounted() {
    setTimeout(this.loadInfo, 1000)
  },
  methods: {
    loadInfo() {
      sayHello().then(res => {
        const data = res.data
        this.info = data
      })
    }
  }
}
</script>
