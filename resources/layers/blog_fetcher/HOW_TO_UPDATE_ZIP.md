Update the requirements in requirements.txt, then run:

```
docker run -v "$PWD":/var/task "lambci/lambda:build-python3.8" /bin/sh -c "pip install -r requirements.txt --upgrade -t python/lib/python3.8/site-packages/; exit"
zip -r layer.zip python
```