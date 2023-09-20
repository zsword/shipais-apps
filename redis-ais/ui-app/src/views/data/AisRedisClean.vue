<template>
    <div>
        <div ref="aiscount" class="ais-chart"></div>
        <div v-if="loading">数据加载中。。。。。。</div>
        <div v-else>
                    <div>{{dataInfo}}</div>
        <fieldset>
            <legend>AIS数据清理</legend>
            <label v-for="cl in cleans" :key="cl.value"><input type="radio" name="cleanType" :value="cl.value" v-model="cleanType" />{{cl.label}}</label>
        </fieldset>
        <button @click="loadData">刷新数据</button> | <button @click="cleanData">清理数据</button>
        </div>
    </div>
</template>

<script>
import { countByTimes, cleanData } from '../../api/data/aisdata'
import * as echarts from 'echarts'

const Colors = [
  '#00008b',
  '#f00',
  '#ffde00',
  '#002a8f',
  '#003580',
  '#ed2939',
  '#000',
  '#003897',
  '#f93',
  '#bc002d',
  '#024fa2',
  '#000',
  '#00247d',
  '#ef2b2d',
  '#dc143c',
  '#d52b1e',
  '#e30a17',
  '#00247d',
  '#b22234'
]

export default {
    data() {
        return {
            loading: true,
            cleans: [{
                label: "清理过期数据Key",
                value: 'dx'
            }, {
                label: '清理过期Geo',
                value: '_geo'
            }, {
                label: '清理过期时间Key',
                value: '_t'            
            }],
            dataInfo: {},
            cleanType: []
        }
    },
    created() {
        this.loadData()
    },
    mounted() {
        this.echart = echarts.init(this.$refs.aiscount)
    },
    methods: {
        loadData() {
            this.loading = true
            countByTimes().then(res=>{
                const result = res.data
                if(result.code!==200) {
                    alert('LoadData Error: '+result.message)
                    return
                }
                const data = result.data
                Object.assign(this.dataInfo, data)
                this.loading = false
                this.loadChart(data)
            })
        },
        loadChart(data) {
            const source = []
            for(let p in data) {
                const value = [p, data[p]]
                if(p.indexOf('all')==0) {
                    source.splice(0, 0, value)
                } else {
                    source.push(value)
                }
            }
            const chart = this.echart
            chart.setOption({
                legend: {},
                tooltip: {},
                dataset: {
                    source
                },
                xAxis: { type: 'category' },
                yAxis: {},
                series: [{ 
                    type: 'bar',
                    itemStyle: {
                        color: function (param) {
                            const value = param.value[1]
                            let idx = value/10000
                            return Colors[idx]|| '#5470c6'
                        }
                    },                    
                }]
            })
        },
        cleanData() {
            const cleanType = this.cleanType
            if(!cleanType) return false
            return cleanData({type:cleanType}).then(res=>{
                const result = res.data
                if(result.code!=200) {
                    alert('CleanData Error: '+result.message)
                    return
                }
                this.loadData()
            })
        }
    },
}
</script>

<style>
.ais-chart {
    height: 350px;
    width: 100%;
    border: 1px solid;
}
</style>