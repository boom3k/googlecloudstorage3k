package googlecloudstorage4go

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/option"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type CloudStorageAPI struct {
	Client     *storage.Client
	AdminEmail string
	Domain     string
}

type DownloadedFile struct {
	LocalData    []byte
	ParentFolder string
	FullFilePath string
	Extension    string
	Name         string
	Size         int64
	IsDir        bool
	Mode         fs.FileMode
	Sys          interface{}
	CreateTime   time.Time
	ModTime      time.Time
	Bucket       string
	BucketBlob   BucketBlob
}

type BucketBlob struct {
	Data     []byte
	BlobName string
	Bucket   string
}

func Builder(adminEmail string, serviceAccountKey []byte, ctx context.Context) *CloudStorageAPI {
	newClient, err := storage.NewClient(ctx, option.WithCredentialsJSON(serviceAccountKey))
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	newCloudStorageAPI := &CloudStorageAPI{Client: newClient, AdminEmail: adminEmail, Domain: strings.Split(adminEmail, "@")[1]}
	log.Printf("CloudstorageAPI initialized --> [%v]\n", &newCloudStorageAPI)
	return newCloudStorageAPI
}

func (receiver *CloudStorageAPI) Upload(bucketName, filename string, data []byte, ctx context.Context) {
	reader := bytes.NewReader(data)

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	writer := receiver.Client.Bucket(bucketName).Object(filename).NewWriter(ctx)
	defer writer.Close()

	_, err := io.Copy(writer, reader)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	log.Printf("Uploaded [%s] of %d", filename, reader.Size())

}

func (receiver *CloudStorageAPI) UploadByPath(bucketName, fileToUploadPath string, ctx context.Context) {
	fileData, err := ioutil.ReadFile(fileToUploadPath)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	fileName := filepath.Base(fileToUploadPath)
	receiver.Upload(bucketName, fileName, fileData, ctx)
}

func (receiver *CloudStorageAPI) GetBlobFromBucket(bucketName, objectName string, ctx context.Context) (*BucketBlob, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	downloadedFileReader, err := receiver.Client.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	defer downloadedFileReader.Close()

	downloadedData, err := ioutil.ReadAll(downloadedFileReader)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &BucketBlob{Data: downloadedData, BlobName: objectName, Bucket: bucketName}, nil

}

func (receiver *CloudStorageAPI) DownloadToOS(bucketName, objectName, destinationPath string, ctx context.Context) (*DownloadedFile, error) {
	bucketBlob, err := receiver.GetBlobFromBucket(bucketName, objectName, ctx)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	if destinationPath[len(destinationPath)-1:] != string(os.PathSeparator) {
		destinationPath += string(os.PathSeparator)
	}

	fullFilePath := destinationPath + objectName

	if _, err := os.Stat(destinationPath); os.IsNotExist(err) {
		err = os.Mkdir(destinationPath, os.ModePerm)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}

	err = os.WriteFile(fullFilePath, bucketBlob.Data, os.ModePerm)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	localData, err := os.ReadFile(fullFilePath)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	file, err := os.Open(fullFilePath)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return &DownloadedFile{
		BucketBlob:   *bucketBlob,
		Name:         fileStat.Name(),
		Size:         fileStat.Size(),
		IsDir:        fileStat.IsDir(),
		Mode:         fileStat.Mode(),
		Sys:          fileStat.Sys(),
		ModTime:      fileStat.ModTime(),
		CreateTime:   time.Now(),
		LocalData:    localData,
		ParentFolder: destinationPath,
		FullFilePath: fullFilePath,
		Extension:    filepath.Ext(fileStat.Name()),
	}, err

}
