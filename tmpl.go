package mysql

import "text/template"

var tmplFuncs = template.FuncMap{
	"marshal": func(x any) (string, error) {
		b, err := marshal(x, 0)
		if err != nil {
			return "", err
		}

		return string(b), nil
	},
}
