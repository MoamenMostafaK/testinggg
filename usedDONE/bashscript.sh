#!/bin/bash
echo "hello from bashscript"

sudo apt-get install cowsay -y
cowsay -f dragon "Run for cover,iam a Dragon ... RAWA" >> dragon.txt
grep -i "dragon" dragon.txt
cat dragon.txt
ls 