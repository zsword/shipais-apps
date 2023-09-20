import axios from "axios"
import appconfig from "../consts/appconfig"

const service = axios.create({
    baseURL: appconfig.apiRoot||'/api'
})

export default service