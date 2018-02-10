
rm -rf output outputSync 1 2 3 4 5 6 7 8

: <<'END'
sudo yum install -y git
wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.2/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
END
nvm use 9

# Create a separate folder and download the repo to each folder
mkdir output outputSync 1 #2 3 4 5 6 7 8

cd 1
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
node --max_old_space_size=4096 direct_to_s3.js al ak #az ar ca co ct
cd ..


# node --max_old_space_size=4096 direct_to_s3.js al ak az ar ca co ct
# node --max_old_space_size=4096 direct_to_s3.js de dc fl ga hi id il
# node --max_old_space_size=4096 direct_to_s3.js in ia ks ky la me md
# node --max_old_space_size=4096 direct_to_s3.js ma mi mn ms mo mt ne
# node --max_old_space_size=4096 direct_to_s3.js nv nh nj nm ny nc nd
# node --max_old_space_size=4096 direct_to_s3.js oh ok or pa pr ri sc
# node --max_old_space_size=4096 direct_to_s3.js sd tn tx ut vt va wa
# node --max_old_space_size=4096 direct_to_s3.js wv wi wy us


# sync to s3
# aws s3 sync ../outputSync s3://s3db-acs-1115