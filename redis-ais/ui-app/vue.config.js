/**
 * 配置参考:
 * https://cli.vuejs.org/zh/config/
 */
 const url = 'http://127.0.0.1:8088'
 const productionGzipExtensions = ['js', 'css']

 module.exports = {
   lintOnSave: true,
   publicPath: process.env.NODE_ENV==="production"? "/public" : "",
   productionSourceMap: false,
   chainWebpack: config => {
     const entry = config.entry('app')
     entry
       .add('babel-polyfill')
       .end()
     entry
       .add('classlist-polyfill')
       .end()
   },
   css: {
     // 忽略 CSS order 顺序警告
     extract: { ignoreOrder: true }
   },
   configureWebpack: (config) => {
     if (process.env.NODE_ENV === 'production') {
       // 仅在生产环境下启用该配置
       return {
         performance: {
           // 打包后最大文件大小限制
           maxAssetSize: 1024000
         },
         plugins: [
         ]
       }
     }
   },
   // 配置转发代理
   devServer: {
     disableHostCheck: true,
     port: 8080,
     proxy: {
       '/api': {
         target: url,
         ws: true, // 需要websocket 开启
         pathRewrite: {
           '^/api': '/'
         }
       }
     }
   }
 }
 