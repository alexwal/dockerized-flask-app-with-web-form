# Installation
Run:

`>>> docker build -t myimage .`

Then launch the container as follows:

`>>> docker run --name mycontainer -p 80:80 myimage`

Finally, in a browser, navigate to:

http://0.0.0.0/
