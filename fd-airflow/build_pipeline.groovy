@Library('cicd-library') _

pipeline {
    agent any

    parameters {
        string(name: 'APP_NAME')
        string(name: 'REPO_URL')
        string(name: 'BRANCH_NAME')
    }

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '5'))
        timestamps()
    }

    environment {
        IMAGE        = "lavupraveen/${params.APP_NAME}"
        STATE_DIR    = "/u01/jenkins/jenkins/build-state"
        ARTIFACT_DIR = "/u01/jenkins/jenkins/artifacts/${params.APP_NAME}"
    }

    stages {

        stage('Checkout') {
            steps {
                git url: params.REPO_URL,
                    branch: params.BRANCH_NAME,
                    credentialsId: 'github-token'
            }
        }

        stage('Commit') {
            steps {
                script {
                    env.COMMIT_FULL  = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
                    env.COMMIT_SHORT = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
                }
            }
        }

        stage('Version') {
            steps {
                script {
                    env.VERSION = "v1.0.${env.BUILD_NUMBER}-${env.COMMIT_SHORT}"

                    currentBuild.displayName = "${params.APP_NAME} ${env.VERSION}"
                    currentBuild.description = "${env.COMMIT_SHORT} → ${params.BRANCH_NAME}"

                    echo "📦 Version: ${env.VERSION}"
                }
            }
        }

        // 🔥 HARD SKIP (entire pipeline)
        stage('Skip if already built') {
            steps {
                script {
                    def file = "${env.STATE_DIR}/${params.APP_NAME}/dev.commit"

                    def last = sh(
                        script: "[ -f ${file} ] && cat ${file} || echo none",
                        returnStdout: true
                    ).trim()

                    if (last == env.COMMIT_FULL) {
                        echo "⏭️ Already built → skipping pipeline"
                        currentBuild.description = "Skipped (no changes)"
                        error("STOP_PIPELINE")
                    }
                }
            }
        }

        // 🔥 WAR CACHE CHECK
        stage('Check WAR Cache') {
            steps {
                script {
                    def warPath = "${env.ARTIFACT_DIR}/${env.COMMIT_SHORT}.war"

                    if (fileExists(warPath)) {
                        echo "⏭️ WAR exists → skipping build"
                        env.SKIP_WAR = "true"

                        sh """
                        mkdir -p ${WORKSPACE}/build-output
                        cp ${warPath} ${WORKSPACE}/build-output/app.war
                        """
                    } else {
                        env.SKIP_WAR = "false"
                    }
                }
            }
        }

        stage('Build WAR') {
            when {
                expression { env.SKIP_WAR != "true" }
            }
            steps {
                script {
                    gchatNotify("[${params.APP_NAME}] WAR build started")
                    buildWar(profile: 'dev')
                }
            }
        }

        stage('Persist WAR') {
            when {
                expression { env.SKIP_WAR != "true" }
            }
            steps {
                script {
                    sh """
                    mkdir -p ${env.ARTIFACT_DIR}
                    cp ${WORKSPACE}/build-output/app.war ${env.ARTIFACT_DIR}/${env.COMMIT_SHORT}.war
                    """

                    // 🔥 Keep last 3 WARs only
                    sh """
                    cd ${env.ARTIFACT_DIR}
                    ls -t *.war | tail -n +4 | xargs -r rm -f
                    """
                }
            }
        }

        stage('Fetch Dockerfile') {
            steps {
                script {
                    dir('app-tomcat') {
                        deleteDir()
                        git url: 'https://github.com/praveen-devops-labs/app-tomcat.git',
                            branch: 'main',
                            credentialsId: 'github-token'
                    }
                }
            }
        }

        // 🔥 DOCKER SKIP
        stage('Skip Docker if exists') {
            steps {
                script {
                    def exists = sh(
                        script: "docker manifest inspect ${env.IMAGE}:${env.VERSION} > /dev/null 2>&1 && echo yes || echo no",
                        returnStdout: true
                    ).trim()

                    env.SKIP_DOCKER = (exists == "yes") ? "true" : "false"

                    if (env.SKIP_DOCKER == "true") {
                        echo "⏭️ Docker image exists → skipping build"
                    }
                }
            }
        }

        stage('Docker Build & Push') {
            when {
                expression { env.SKIP_DOCKER != "true" }
            }
            steps {
                script {

                    sh """
                    echo "🔍 Verifying WAR..."
                    ls -ltr ${WORKSPACE}/build-output || true

                    mkdir -p ${WORKSPACE}/app-tomcat/target
                    cp ${WORKSPACE}/build-output/app.war \
                       ${WORKSPACE}/app-tomcat/target/${params.APP_NAME}.war

                    echo "📂 Docker context:"
                    ls -ltr ${WORKSPACE}/app-tomcat/target
                    """

                    dockerBuild(image: env.IMAGE, tag: env.VERSION)

                    withCredentials([usernamePassword(
                        credentialsId: 'dockerhub',
                        usernameVariable: 'USER',
                        passwordVariable: 'PASS'
                    )]) {
                        sh """
                        echo "\$PASS" | docker login -u "\$USER" --password-stdin
                        docker push ${env.IMAGE}:${env.VERSION}
                        docker logout
                        """
                    }
                }
            }
        }

        stage('Update State') {
            steps {
                script {
                    sh """
                    mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
                    echo ${env.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/dev.commit
                    """
                }
            }
        }        
    }

    post {
        success {
            script {
                echo "🚀 Triggering promotion → DEV"

                build job: 'Promotion-Pipeline',
                    wait: false,
                    parameters: [
                        string(name: 'APP_NAME', value: params.APP_NAME),
                        string(name: 'TARGET_ENV', value: 'dev'),
                        string(name: 'COMMIT_FULL', value: env.COMMIT_FULL),
                        string(name: 'VERSION', value: env.VERSION)
                    ]
            }
        }
    }
}




// @Library('cicd-library') _

// pipeline {
//     agent any

//     parameters {
//         string(name: 'APP_NAME')
//         string(name: 'REPO_URL')
//         string(name: 'BRANCH_NAME')
//     }

//     options {
//         disableConcurrentBuilds()
//         buildDiscarder(logRotator(numToKeepStr: '5'))
//         timestamps()
//     }

//     environment {
//         IMAGE        = "lavupraveen/${params.APP_NAME}"
//         STATE_DIR    = "/u01/jenkins/jenkins/build-state"
//         ARTIFACT_DIR = "/u01/jenkins/jenkins/artifacts/${params.APP_NAME}"
//     }

//     stages {

//         stage('Checkout') {
//             steps {
//                 git url: params.REPO_URL,
//                     branch: params.BRANCH_NAME,
//                     credentialsId: 'github-token'
//             }
//         }

//         stage('Commit') {
//             steps {
//                 script {
//                     env.COMMIT_FULL  = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
//                     env.COMMIT_SHORT = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
//                 }
//             }
//         }

//         stage('Version') {
//             steps {
//                 script {
//                     env.VERSION = "v1.0.${env.BUILD_NUMBER}-${env.COMMIT_SHORT}"

//                     currentBuild.displayName = "${params.APP_NAME} ${env.VERSION}"
//                     currentBuild.description = "${env.COMMIT_SHORT} → ${params.BRANCH_NAME}"

//                     echo "📦 Version: ${env.VERSION}"
//                 }
//             }
//         }

//         // 🔥 HARD SKIP (entire pipeline)
//         stage('Skip if already built') {
//             steps {
//                 script {
//                     def file = "${env.STATE_DIR}/${params.APP_NAME}/dev.commit"

//                     def last = sh(
//                         script: "[ -f ${file} ] && cat ${file} || echo none",
//                         returnStdout: true
//                     ).trim()

//                     if (last == env.COMMIT_FULL) {
//                         echo "⏭️ Already built → skipping pipeline"
//                         currentBuild.description = "Skipped (no changes)"
//                         error("STOP_PIPELINE")
//                     }
//                 }
//             }
//         }

//         // 🔥 WAR CACHE CHECK
//         stage('Check WAR Cache') {
//             steps {
//                 script {
//                     def warPath = "${env.ARTIFACT_DIR}/${env.COMMIT_SHORT}.war"

//                     if (fileExists(warPath)) {
//                         echo "⏭️ WAR exists → skipping build"
//                         env.SKIP_WAR = "true"

//                         sh """
//                         mkdir -p ${WORKSPACE}/build-output
//                         cp ${warPath} ${WORKSPACE}/build-output/app.war
//                         """
//                     } else {
//                         env.SKIP_WAR = "false"
//                     }
//                 }
//             }
//         }

//         stage('Build WAR') {
//             when {
//                 expression { env.SKIP_WAR != "true" }
//             }
//             steps {
//                 script {
//                     gchatNotify("[${params.APP_NAME}] WAR build started")
//                     buildWar(profile: 'dev')
//                 }
//             }
//         }

//         stage('Persist WAR') {
//             when {
//                 expression { env.SKIP_WAR != "true" }
//             }
//             steps {
//                 script {
//                     sh """
//                     mkdir -p ${env.ARTIFACT_DIR}
//                     cp ${WORKSPACE}/build-output/app.war ${env.ARTIFACT_DIR}/${env.COMMIT_SHORT}.war
//                     """

//                     // 🔥 Keep last 3 WARs only
//                     sh """
//                     cd ${env.ARTIFACT_DIR}
//                     ls -t *.war | tail -n +4 | xargs -r rm -f
//                     """
//                 }
//             }
//         }

//         stage('Fetch Dockerfile') {
//             steps {
//                 script {
//                     dir('app-tomcat') {
//                         deleteDir()
//                         git url: 'https://github.com/praveen-devops-labs/app-tomcat.git',
//                             branch: 'main',
//                             credentialsId: 'github-token'
//                     }
//                 }
//             }
//         }

//         // 🔥 DOCKER SKIP
//         stage('Skip Docker if exists') {
//             steps {
//                 script {
//                     def exists = sh(
//                         script: "docker manifest inspect ${env.IMAGE}:${env.VERSION} > /dev/null 2>&1 && echo yes || echo no",
//                         returnStdout: true
//                     ).trim()

//                     env.SKIP_DOCKER = (exists == "yes") ? "true" : "false"

//                     if (env.SKIP_DOCKER == "true") {
//                         echo "⏭️ Docker image exists → skipping build"
//                     }
//                 }
//             }
//         }

//         stage('Docker Build & Push') {
//             when {
//                 expression { env.SKIP_DOCKER != "true" }
//             }
//             steps {
//                 script {

//                     sh """
//                     echo "🔍 Verifying WAR..."
//                     ls -ltr ${WORKSPACE}/build-output || true

//                     mkdir -p ${WORKSPACE}/app-tomcat/target
//                     cp ${WORKSPACE}/build-output/app.war \
//                        ${WORKSPACE}/app-tomcat/target/${params.APP_NAME}.war

//                     echo "📂 Docker context:"
//                     ls -ltr ${WORKSPACE}/app-tomcat/target
//                     """

//                     dockerBuild(image: env.IMAGE, tag: env.VERSION)

//                     withCredentials([usernamePassword(
//                         credentialsId: 'dockerhub',
//                         usernameVariable: 'USER',
//                         passwordVariable: 'PASS'
//                     )]) {
//                         sh """
//                         echo "\$PASS" | docker login -u "\$USER" --password-stdin
//                         docker push ${env.IMAGE}:${env.VERSION}
//                         docker logout
//                         """
//                     }
//                 }
//             }
//         }

//         stage('Update State') {
//             steps {
//                 script {
//                     sh """
//                     mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
//                     echo ${env.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/dev.commit
//                     """
//                 }
//             }
//         }

//         stage('Deploy DEV') {
//             steps {
//                 build job: 'Deploy-Pipeline',
//                     parameters: [
//                         string(name: 'APP_NAME', value: params.APP_NAME),
//                         string(name: 'TARGET_ENV', value: 'dev'),
//                         string(name: 'COMMIT_FULL', value: env.COMMIT_FULL),
//                         string(name: 'VERSION', value: env.VERSION)
//                     ]
//             }
//         }
//     }

//     post {
//         always {
//             cleanWs()
//         }
//         failure {
//             script {
//                 if (currentBuild.description?.contains("Skipped")) {
//                     currentBuild.result = "SUCCESS"
//                 }
//             }
//         }
//     }
// }


// @Library('cicd-library') _

// pipeline {
//     agent any

//     parameters {
//         string(name: 'APP_NAME')
//         string(name: 'REPO_URL')
//         string(name: 'BRANCH_NAME')
//     }

//     options {
//         disableConcurrentBuilds() // ✅ allow parallel builds
//         buildDiscarder(logRotator(numToKeepStr: '5'))
//         timestamps()
//     }

//     environment {
//         IMAGE = "lavupraveen/${params.APP_NAME}"
//         STATE_DIR = "/u01/jenkins/jenkins/build-state"
//     }

//     stages {

//         stage('Checkout') {
//             steps {
//                 git url: params.REPO_URL,
//                     branch: params.BRANCH_NAME,
//                     credentialsId: 'github-token'
//             }
//         }

//         stage('Commit') {
//             steps {
//                 script {
//                     env.COMMIT_FULL = sh(script: "git rev-parse HEAD", returnStdout: true).trim()
//                     env.COMMIT_SHORT = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
//                 }
//             }
//         }

//         stage('Version') {
//             steps {
//                 script {
//                     env.VERSION = "v1.0.${env.BUILD_NUMBER}-${env.COMMIT_SHORT}"

//                     currentBuild.displayName = "${params.APP_NAME} ${env.VERSION}"
//                     currentBuild.description = "${env.COMMIT_SHORT} → ${params.BRANCH_NAME}"

//                     echo "📦 Version: ${env.VERSION}"
//                 }
//             }
//         }

//         // 🔥 FULL PIPELINE SKIP
//         stage('Skip if already built') {
//             steps {
//                 script {

//                     def file = "${env.STATE_DIR}/${params.APP_NAME}/dev.commit"

//                     def last = sh(
//                         script: "[ -f ${file} ] && cat ${file} || echo none",
//                         returnStdout: true
//                     ).trim()

//                     if (last == env.COMMIT_FULL) {
//                         echo "⏭️ Already built → skipping pipeline"
//                         currentBuild.result = "SUCCESS"
//                         return
//                     }
//                 }
//             }
//         }        
                
        
//         stage('Build WAR') {
//             steps {
//                 script {
//                     gchatNotify("[${params.APP_NAME}] WAR build started")
//                     buildWar(profile: 'dev')
//                 }
//             }
//         }

//         stage('Fetch Dockerfile') {
//             steps {
//                 script {
//                     dir('app-tomcat') {
//                         deleteDir()
//                         git url: 'https://github.com/praveen-devops-labs/app-tomcat.git',
//                             branch: 'main',
//                             credentialsId: 'github-token'
//                     }
//                 }
//             }
//         }

//         stage('Docker Build & Push') {
//             steps {
//                 script {

//                     sh """
//                     echo "🔍 Verifying WAR..."
//                     ls -ltr ${WORKSPACE}/build-output || true

//                     mkdir -p ${WORKSPACE}/app-tomcat/target
//                     cp ${WORKSPACE}/build-output/app.war ${WORKSPACE}/app-tomcat/target/app1.war

//                     echo "📂 Docker context:"
//                     ls -ltr ${WORKSPACE}/app-tomcat/target
//                     """

//                     dockerBuild(image: env.IMAGE, tag: env.VERSION)

//                     withCredentials([usernamePassword(
//                         credentialsId: 'dockerhub',
//                         usernameVariable: 'USER',
//                         passwordVariable: 'PASS'
//                     )]) {
//                         sh """
//                         echo "\$PASS" | docker login -u "\$USER" --password-stdin
//                         docker push ${env.IMAGE}:${env.VERSION}
//                         docker logout
//                         """
//                     }
//                 }
//             }
//         }

//         // 🔥 FINAL SUCCESS CHECKPOINT
//         stage('Update State') {
//             steps {
//                 script {
//                     sh """
//                     mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
//                     echo ${env.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/dev.commit
//                     """
//                 }
//             }
//         }

//         stage('Deploy DEV') {
//             steps {
//                 build job: 'Deploy-Pipeline',
//                     parameters: [
//                         string(name: 'APP_NAME', value: params.APP_NAME),
//                         string(name: 'TARGET_ENV', value: 'dev'),
//                         string(name: 'COMMIT_FULL', value: env.COMMIT_FULL),
//                         string(name: 'VERSION', value: env.VERSION)
//                     ]
//             }
//         }
//     }

//     post {
//         always {
//             cleanWs()
//         }
//     }
// }