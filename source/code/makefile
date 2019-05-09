ifndef bucket
bucket = mnp-devops
endif

pydirs= actions handlers scheduling services util configuration boto_retry
py=$(foreach dir, $(pydirs), $(wildcard $(dir)/*.py))

templates=$(wildcard $(base)/cloudformation/*.template)

version=`cat version.txt`
dest=../deploy/$(version)
zip = $(dest)/ops-automator-$(version).zip
pwd=`pwd`

s3bucket=s3://$(bucket)/ops-automator/$(version)/

build: lambda cfn

lambda:$(py) main.py version.txt
	mkdir -p $(dest) 
	zip  $(zip) version.txt
	mv main.py main.py.org
	sed s/%version%/$(version)/g main.py.org > main.py
	zip  $(zip) main.py config.py
	rm main.py
	mv main.py.org main.py
	zip -r $(zip)  $(py) models version.txt
	zip -r -q $(zip) pytz

#	python build-task-scheduler-template.py ../cloudformation/ops-automator.template $(version) $(bucket) > $(dest)/ops-automator-$(version).template
cfn:version.txt $(templates)
	mkdir -p $(dest)
	mv ../cloudformation/ops-automator.template ../cloudformation/ops-automator.template.org
	sed -e s/%version%/$(version)/g -e s/%bucket%/$(bucket)/g ../cloudformation/ops-automator.template.org > ../cloudformation/ops-automator.template
	cp ../cloudformation/ops-automator.template  $(dest)/ops-automator-$(version).template
	rm ../cloudformation/ops-automator.template
	mv ../cloudformation/ops-automator.template.org ../cloudformation/ops-automator.template
	cp $(dest)/ops-automator-$(version).template  $(dest)/ops-automator-latest.template
	cp $(dest)/ops-automator-latest.template ../../deployment/ops-automator-latest.template
	cp $(zip) ../../deployment/ops-automator-$(version).zip

deploy: build
	aws s3 cp $(dest) $(s3bucket) --recursive
#	python update-build-number.py version.txt

clean:
	rm -f $(dest)/*

all: build deploy
