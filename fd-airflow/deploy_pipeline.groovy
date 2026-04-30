@Library('cicd-library') _

pipeline {
    agent any

    parameters {
        string(name: 'APP_NAME')
        choice(name: 'TARGET_ENV', choices: ['dev','qa','uat','prod'])
        string(name: 'COMMIT_FULL')
        string(name: 'VERSION')
    }

    options {
        disableConcurrentBuilds(abortPrevious: true)
        buildDiscarder(logRotator(numToKeepStr: '5'))
        timestamps()
    }

    environment {
        IMAGE = "lavupraveen/${params.APP_NAME}:${params.VERSION}"
    }

    stages {

        stage('Deploy') {
            steps {
                script {

                    gchatNotify("🚀 Deploying ${params.APP_NAME} → ${params.TARGET_ENV}")

                    def script = libraryResource('scripts/deploy.sh')
                    writeFile file: 'deploy.sh', text: script

                    sh """
                    chmod +x deploy.sh
                    ./deploy.sh ${params.TARGET_ENV} ${env.IMAGE} ${params.APP_NAME}
                    """
                }
            }
        }
    }

    post {
        success {
            gchatNotify("✅ Deploy SUCCESS → ${params.APP_NAME} (${params.TARGET_ENV})")
        }
        failure {
            gchatNotify("❌ Deploy FAILED → ${params.APP_NAME} (${params.TARGET_ENV})")
        }
    }
}



// @Library('cicd-library') _

// pipeline {
//     agent any

//     parameters {
//         string(name: 'APP_NAME')
//         choice(name: 'TARGET_ENV', choices: ['dev','qa','uat','prod'])
//         string(name: 'COMMIT_FULL')
//         string(name: 'VERSION')
//     }

//     options {
//         disableConcurrentBuilds()
//         buildDiscarder(logRotator(numToKeepStr: '5'))
//         timestamps()
//     }

//     environment {
//         IMAGE = "lavupraveen/${params.APP_NAME}:${params.VERSION}"
//         STATE_DIR = "/u01/jenkins/jenkins/build-state"
//     }

//     stages {

//         stage('Deploy') {
//             steps {
//                 script {
//                     gchatNotify("[" + params.APP_NAME + "] Deploy → " + params.TARGET_ENV)

//                     deploy(
//                         env: params.TARGET_ENV,
//                         image: env.IMAGE,
//                         app: params.APP_NAME
//                     )
//                 }
//             }
//         }

//         stage('Update State') {
//             steps {
//                 script {
//                     sh """
//                     mkdir -p ${env.STATE_DIR}/${params.APP_NAME}
//                     echo ${params.COMMIT_FULL} > ${env.STATE_DIR}/${params.APP_NAME}/${params.TARGET_ENV}.commit
//                     """
//                 }
//             }
//         }

//         stage('History') {
//             steps {
//                 script {
//                     writeHistory([
//                         app: params.APP_NAME,
//                         env: params.TARGET_ENV,
//                         commit: params.COMMIT_FULL,
//                         version: params.VERSION,
//                         type: "DEPLOY"
//                     ])
//                 }
//             }
//         }
//     }

//     post {
//         success {
//             gchatNotify("[${params.APP_NAME}] Deploy SUCCESS → ${params.TARGET_ENV}")

//             script {

//                 def nextEnv = [
//                     dev: 'qa',
//                     qa: 'uat'
//                 ][params.TARGET_ENV]

//                 // 🔥 Auto-promote only for dev → qa → uat
//                 if (nextEnv) {
//                     echo "🚀 Auto promoting to ${nextEnv}"

//                     build job: 'Promotion-Pipeline',
//                         wait: false,
//                         parameters: [
//                             string(name: 'APP_NAME', value: params.APP_NAME),
//                             string(name: 'TARGET_ENV', value: nextEnv),
//                             string(name: 'COMMIT_FULL', value: params.COMMIT_FULL),
//                             string(name: 'VERSION', value: params.VERSION)
//                         ]
//                 }
//             }
//         }

//         failure {
//             gchatNotify("[${params.APP_NAME}] Deploy FAILED → ${params.TARGET_ENV}")
//         }
//     }
// }