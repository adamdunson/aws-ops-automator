ifndef bucket
bucket = mnp-devops
endif

pydirs= actions boto_retry builders configuration handlers helpers metrics outputs scheduling services tagging
py=$(foreach dir, $(pydirs), $(wildcard $(dir)/*.py))

templates=$(wildcard $(base)/cloudformation/*.template)

version= `cat version.txt`
dest= ../deploy/$(version)
zip= $(dest)/ops-automator-$(version).zip
cwzip= $(dest)/cloudwatch-handler-$(version).zip
pwd= `pwd`

s3bucket= s3://$(bucket)/ops-automator/$(version)

build: lambda cwlambda cfn

lambda:$(py) main.py version.txt
	mkdir -p $(dest)
	zip  $(zip) version.txt
	zip  $(zip) main.py
	zip -r $(zip)  $(py) models version.txt
	zip -r -q $(zip) pytz

cwlambda: cloudwatch_queue_handler_lambda.py version.txt
	mkdir -p $(dest)
	zip  $(cwzip) version.txt
	mv cloudwatch_queue_handler_lambda.py cloudwatch_queue_handler_lambda.py.org
	sed s/%version%/$(version)/g cloudwatch_queue_handler_lambda.py.org > cloudwatch_queue_handler_lambda.py
	zip  $(cwzip) cloudwatch_queue_handler_lambda.py
	rm cloudwatch_queue_handler_lambda.py
	mv cloudwatch_queue_handler_lambda.py.org cloudwatch_queue_handler_lambda.py

# 	python build-task-scheduler-template.py ../cloudformation/ops-automator.template $(version) $(bucket) > $(dest)/ops-automator-$(version).template
cfn: version.txt $(templates)
	mkdir -p $(dest)
	mv ../cloudformation/ops-automator.template ../cloudformation/ops-automator.template.org
	sed -e s/%version%/$(version)/g -e s/%bucket%/$(bucket)/g ../cloudformation/ops-automator.template.org > ../cloudformation/ops-automator.template
	cp ../cloudformation/ops-automator.template $(dest)/ops-automator-$(version).template
	rm ../cloudformation/ops-automator.template
	mv ../cloudformation/ops-automator.template.org ../cloudformation/ops-automator.template
	cp $(dest)/ops-automator-$(version).template  $(dest)/ops-automator-latest.template
	cp $(dest)/ops-automator-latest.template ../../deployment/ops-automator-latest.template
	cp $(zip) ../../deployment/ops-automator-$(version).zip
	cp $(cwzip) ../../deployment/cloudwatch-handler-$(version).zip

#	python update-build-number.py version.txt
deploy: build
	aws s3 cp $(dest) $(s3bucket) --recursive

clean:
	rm -f $(dest)/*

all: build deploy
