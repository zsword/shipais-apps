package model

type dataResult struct {
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
}

func NewResult(code int, data interface{}, message string) (result dataResult) {
	result = dataResult{
		code,
		data,
		message,
	}
	return
}

func (res *dataResult) SetData(obj interface{}) {
	res.Data = obj
}

func (res *dataResult) SetError(messsage string, err error) {
	res.Code = 500
	if err != nil {
		messsage = messsage + err.Error()
	}
	res.Message = messsage
}
